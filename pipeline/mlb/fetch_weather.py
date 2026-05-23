"""
Fetch game-time weather for MLB ballparks.
Uses Open-Meteo API (free, no auth required).
Stores temperature, wind speed/direction, humidity, precipitation.

Wind effects on HR probability:
- Out to CF (180 deg from wind source): +8% HR boost per 10mph
- In from CF (0 deg): -8% HR penalty per 10mph
- Temperature: +1.5% HR per 10 deg F above 70 deg F
"""
import sys
sys.path.insert(0, '/app')

import argparse
import math
import httpx
from datetime import date
from database import SessionLocal
from sqlalchemy import text

BALLPARKS = {
    'ARI': {'lat': 33.4453, 'lon': -112.0667, 'name': 'Chase Field',
            'cf_direction': 180, 'elevation': 1086},
    'ATL': {'lat': 33.8907, 'lon': -84.4677, 'name': 'Truist Park',
            'cf_direction': 35, 'elevation': 1050},
    'BAL': {'lat': 39.2838, 'lon': -76.6218, 'name': 'Camden Yards',
            'cf_direction': 80, 'elevation': 20},
    'BOS': {'lat': 42.3467, 'lon': -71.0972, 'name': 'Fenway Park',
            'cf_direction': 95, 'elevation': 20},
    'CHC': {'lat': 41.9484, 'lon': -87.6553, 'name': 'Wrigley Field',
            'cf_direction': 90, 'elevation': 595},
    'CWS': {'lat': 41.8299, 'lon': -87.6338, 'name': 'Guaranteed Rate',
            'cf_direction': 5, 'elevation': 595},
    'CIN': {'lat': 39.0979, 'lon': -84.5082, 'name': 'Great American',
            'cf_direction': 15, 'elevation': 490},
    'CLE': {'lat': 41.4962, 'lon': -81.6852, 'name': 'Progressive Field',
            'cf_direction': 30, 'elevation': 650},
    'COL': {'lat': 39.7559, 'lon': -104.9942, 'name': 'Coors Field',
            'cf_direction': 10, 'elevation': 5200},
    'DET': {'lat': 42.3390, 'lon': -83.0485, 'name': 'Comerica Park',
            'cf_direction': 50, 'elevation': 600},
    'HOU': {'lat': 29.7573, 'lon': -95.3555, 'name': 'Minute Maid Park',
            'cf_direction': 25, 'elevation': 40},
    'KC':  {'lat': 39.0517, 'lon': -94.4803, 'name': 'Kauffman Stadium',
            'cf_direction': 5, 'elevation': 750},
    'LAA': {'lat': 33.8003, 'lon': -117.8827, 'name': 'Angel Stadium',
            'cf_direction': 0, 'elevation': 160},
    'LAD': {'lat': 34.0739, 'lon': -118.2400, 'name': 'Dodger Stadium',
            'cf_direction': 355, 'elevation': 515},
    'MIA': {'lat': 25.7781, 'lon': -80.2197, 'name': 'loanDepot Park',
            'cf_direction': 355, 'elevation': 6},
    'MIL': {'lat': 43.0280, 'lon': -87.9712, 'name': 'American Family Field',
            'cf_direction': 10, 'elevation': 635},
    'MIN': {'lat': 44.9817, 'lon': -93.2777, 'name': 'Target Field',
            'cf_direction': 7, 'elevation': 830},
    'NYM': {'lat': 40.7571, 'lon': -73.8458, 'name': 'Citi Field',
            'cf_direction': 55, 'elevation': 20},
    'NYY': {'lat': 40.8296, 'lon': -73.9262, 'name': 'Yankee Stadium',
            'cf_direction': 25, 'elevation': 55},
    'OAK': {'lat': 37.7516, 'lon': -122.2005, 'name': 'Oakland Coliseum',
            'cf_direction': 315, 'elevation': 25},
    'PHI': {'lat': 39.9061, 'lon': -75.1665, 'name': 'Citizens Bank Park',
            'cf_direction': 10, 'elevation': 20},
    'PIT': {'lat': 40.4469, 'lon': -80.0057, 'name': 'PNC Park',
            'cf_direction': 10, 'elevation': 730},
    'SD':  {'lat': 32.7076, 'lon': -117.1570, 'name': 'Petco Park',
            'cf_direction': 315, 'elevation': 15},
    'SF':  {'lat': 37.7786, 'lon': -122.3893, 'name': 'Oracle Park',
            'cf_direction': 90, 'elevation': 10},
    'SEA': {'lat': 47.5914, 'lon': -122.3325, 'name': 'T-Mobile Park',
            'cf_direction': 5, 'elevation': 20},
    'STL': {'lat': 38.6226, 'lon': -90.1928, 'name': 'Busch Stadium',
            'cf_direction': 10, 'elevation': 465},
    'TB':  {'lat': 27.7683, 'lon': -82.6534, 'name': 'Tropicana Field',
            'cf_direction': 0, 'elevation': 10},
    'TEX': {'lat': 32.7512, 'lon': -97.0832, 'name': 'Globe Life Field',
            'cf_direction': 15, 'elevation': 550},
    'TOR': {'lat': 43.6414, 'lon': -79.3894, 'name': 'Rogers Centre',
            'cf_direction': 5, 'elevation': 250},
    'WSH': {'lat': 38.8730, 'lon': -77.0074, 'name': 'Nationals Park',
            'cf_direction': 5, 'elevation': 25},
}

INDOOR_PARKS = {'MIA', 'TB', 'HOU', 'MIL', 'ARI', 'TOR', 'SEA', 'MIN', 'TEX'}

TEAM_ID_MAP = {
    109: 'ARI', 144: 'ATL', 110: 'BAL', 111: 'BOS',
    112: 'CHC', 145: 'CWS', 113: 'CIN', 114: 'CLE',
    115: 'COL', 116: 'DET', 117: 'HOU', 118: 'KC',
    108: 'LAA', 119: 'LAD', 146: 'MIA', 158: 'MIL',
    142: 'MIN', 121: 'NYM', 147: 'NYY', 133: 'OAK',
    143: 'PHI', 134: 'PIT', 135: 'SD',  137: 'SF',
    136: 'SEA', 138: 'STL', 139: 'TB',  140: 'TEX',
    141: 'TOR', 120: 'WSH',
}


def create_weather_table(db):
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS mlb.game_weather (
            id BIGSERIAL PRIMARY KEY,
            game_pk INTEGER NOT NULL,
            game_date DATE,
            team_abbrev VARCHAR(5),
            venue_name VARCHAR(100),
            is_indoor BOOLEAN DEFAULT FALSE,
            temperature_f FLOAT,
            feels_like_f FLOAT,
            humidity_pct FLOAT,
            wind_speed_mph FLOAT,
            wind_direction_deg FLOAT,
            wind_gust_mph FLOAT,
            precipitation_in FLOAT,
            weather_condition VARCHAR(50),
            wind_hr_modifier FLOAT,
            temp_hr_modifier FLOAT,
            combined_hr_modifier FLOAT,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(game_pk)
        )
    """))
    db.commit()


def compute_wind_modifier(wind_speed: float, wind_dir: float,
                           cf_direction: float) -> float:
    """
    Compute HR probability modifier based on wind.

    wind_dir: degrees wind is coming FROM (meteorological)
    cf_direction: degrees to center field FROM home plate
    """
    if wind_speed < 2:
        return 1.0

    ball_wind_dir = (wind_dir + 180) % 360
    angle_diff = abs(ball_wind_dir - cf_direction) % 360
    if angle_diff > 180:
        angle_diff = 360 - angle_diff

    cf_component = math.cos(math.radians(angle_diff))
    modifier = 1.0 + (cf_component * wind_speed * 0.008)
    return round(max(0.85, min(1.15, modifier)), 3)


def compute_temp_modifier(temp_f: float) -> float:
    """Warmer air is less dense — ball travels farther. ~1.5% per 10F above 70F baseline."""
    modifier = 1.0 + ((temp_f - 70.0) / 10) * 0.015
    return round(max(0.92, min(1.08, modifier)), 3)


def get_weather_condition(wmo_code: int) -> str:
    """Convert WMO weather code to description."""
    if wmo_code == 0: return "Clear"
    if wmo_code <= 3: return "Partly Cloudy"
    if wmo_code <= 49: return "Foggy"
    if wmo_code <= 59: return "Drizzle"
    if wmo_code <= 69: return "Rain"
    if wmo_code <= 79: return "Snow"
    if wmo_code <= 82: return "Rain Showers"
    if wmo_code <= 99: return "Thunderstorm"
    return "Unknown"


def fetch_weather_for_game(game_pk: int, game_date: str,
                            team_abbrev: str, db) -> dict:
    """Fetch and store weather for a specific game."""
    park = BALLPARKS.get(team_abbrev)
    if not park:
        return {}

    is_indoor = team_abbrev in INDOOR_PARKS

    if is_indoor:
        db.execute(text("""
            INSERT INTO mlb.game_weather (
                game_pk, game_date, team_abbrev, venue_name, is_indoor,
                temperature_f, wind_speed_mph, wind_hr_modifier,
                temp_hr_modifier, combined_hr_modifier
            ) VALUES (
                :game_pk, :game_date, :team_abbrev, :venue_name, TRUE,
                72.0, 0.0, 1.0, 1.0, 1.0
            )
            ON CONFLICT (game_pk) DO NOTHING
        """), {
            "game_pk": game_pk,
            "game_date": game_date,
            "team_abbrev": team_abbrev,
            "venue_name": park['name'],
        })
        db.commit()
        return {"is_indoor": True, "combined_hr_modifier": 1.0}

    try:
        r = httpx.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": park['lat'],
                "longitude": park['lon'],
                "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,winddirection_10m,windgusts_10m,precipitation,weathercode",
                "temperature_unit": "fahrenheit",
                "windspeed_unit": "mph",
                "precipitation_unit": "inch",
                "start_date": game_date,
                "end_date": game_date,
                "timezone": "America/New_York",
            },
            timeout=10
        )
        r.raise_for_status()
        data = r.json()

        hour_idx = 19  # 7pm ET typical game time
        hourly = data.get('hourly', {})

        temp_f    = float(hourly.get('temperature_2m', [72])[hour_idx])
        humidity  = float(hourly.get('relativehumidity_2m', [50])[hour_idx])
        wind_speed = float(hourly.get('windspeed_10m', [0])[hour_idx])
        wind_dir   = float(hourly.get('winddirection_10m', [0])[hour_idx])
        wind_gust  = float(hourly.get('windgusts_10m', [0])[hour_idx])
        precip     = float(hourly.get('precipitation', [0])[hour_idx])
        wmo_code   = int(hourly.get('weathercode', [0])[hour_idx])

        condition  = get_weather_condition(wmo_code)
        wind_mod   = compute_wind_modifier(wind_speed, wind_dir, park['cf_direction'])
        temp_mod   = compute_temp_modifier(temp_f)
        combined   = round(wind_mod * temp_mod, 3)

        db.execute(text("""
            INSERT INTO mlb.game_weather (
                game_pk, game_date, team_abbrev, venue_name, is_indoor,
                temperature_f, humidity_pct, wind_speed_mph, wind_direction_deg,
                wind_gust_mph, precipitation_in, weather_condition,
                wind_hr_modifier, temp_hr_modifier, combined_hr_modifier
            ) VALUES (
                :game_pk, :game_date, :team_abbrev, :venue_name, FALSE,
                :temp, :humidity, :wind_speed, :wind_dir,
                :wind_gust, :precip, :condition,
                :wind_mod, :temp_mod, :combined
            )
            ON CONFLICT (game_pk) DO UPDATE SET
                temperature_f = EXCLUDED.temperature_f,
                wind_speed_mph = EXCLUDED.wind_speed_mph,
                wind_direction_deg = EXCLUDED.wind_direction_deg,
                wind_hr_modifier = EXCLUDED.wind_hr_modifier,
                temp_hr_modifier = EXCLUDED.temp_hr_modifier,
                combined_hr_modifier = EXCLUDED.combined_hr_modifier,
                fetched_at = NOW()
        """), {
            "game_pk": game_pk, "game_date": game_date,
            "team_abbrev": team_abbrev, "venue_name": park['name'],
            "temp": temp_f, "humidity": humidity,
            "wind_speed": wind_speed, "wind_dir": wind_dir,
            "wind_gust": wind_gust, "precip": precip,
            "condition": condition,
            "wind_mod": wind_mod, "temp_mod": temp_mod, "combined": combined,
        })
        db.commit()

        return {
            "temperature_f": temp_f,
            "wind_speed_mph": wind_speed,
            "wind_direction_deg": wind_dir,
            "condition": condition,
            "wind_hr_modifier": wind_mod,
            "temp_hr_modifier": temp_mod,
            "combined_hr_modifier": combined,
        }

    except Exception as e:
        print(f"  Weather fetch error for {team_abbrev}: {e}")
        return {}


def fetch_today_weather(db):
    """Fetch weather for today's games."""
    today = str(date.today())

    games_sql = text("""
        SELECT g.game_pk, g.game_date, g.home_team_id
        FROM mlb.games g
        WHERE g.game_date = :today
        AND g.game_type = 'R'
        AND NOT EXISTS (
            SELECT 1 FROM mlb.game_weather gw WHERE gw.game_pk = g.game_pk
        )
    """)
    games = db.execute(games_sql, {"today": today}).mappings().all()
    print(f"  Fetching weather for {len(games)} games on {today}")

    for game in games:
        team_abbrev = TEAM_ID_MAP.get(game['home_team_id'])
        if team_abbrev:
            result = fetch_weather_for_game(
                game['game_pk'], str(game['game_date']),
                team_abbrev, db
            )
            if result:
                indoor = result.get('is_indoor', False)
                print(f"    {team_abbrev}: "
                      f"{'Indoor' if indoor else str(result.get('temperature_f','?'))+'F'}"
                      f"{'' if indoor else ', '+str(result.get('wind_speed_mph','?'))+'mph wind'}"
                      f", HR mod: {result.get('combined_hr_modifier', 1.0):.3f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--today", action="store_true")
    parser.add_argument("--game-pk", type=int)
    parser.add_argument("--team", type=str)
    parser.add_argument("--date", type=str)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        create_weather_table(db)
        if args.game_pk and args.team and args.date:
            result = fetch_weather_for_game(args.game_pk, args.date, args.team, db)
            print(result)
        else:
            fetch_today_weather(db)
    finally:
        db.close()
