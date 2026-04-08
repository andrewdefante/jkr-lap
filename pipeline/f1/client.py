"""
F1 API Client

Wraps the Jolpica F1 API (api.jolpi.ca) which is a drop-in
replacement for the deprecated Ergast API. No auth required.

Base URL: https://api.jolpi.ca/ergast/f1/

Endpoints:
  Schedule:    /f1/{season}/
  Results:     /f1/{season}/{round}/results/
  Lap times:   /f1/{season}/{round}/laps/
  Pit stops:   /f1/{season}/{round}/pitstops/
  Qualifying:  /f1/{season}/{round}/qualifying/
  Drivers:     /f1/{season}/drivers/
  Constructors:/f1/{season}/constructors/
"""

import requests
import time
from dataclasses import dataclass
from typing import Optional, Any


@dataclass
class F1Config:
    base_url: str = "https://api.jolpi.ca/ergast/f1"
    timeout: int = 30
    page_size: int = 100      # max results per request
    request_delay: float = 0.2  # be polite to the API


class F1Client:
    """
    Thin client for the Jolpica F1 API.
    Handles pagination automatically.
    All methods return raw parsed JSON.
    """

    def __init__(self, config: F1Config = F1Config()):
        self.config = config

    def _get(self, path: str, params: dict = None) -> Optional[Any]:
        url = f"{self.config.base_url}{path}"
        try:
            response = requests.get(url, params=params, timeout=self.config.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"  [F1Client] Failed: {url}\n  Error: {e}")
            return None

    def _get_all_pages(self, path: str) -> Optional[list]:
        """
        Fetch all pages for a paginated endpoint.
        Jolpica uses limit/offset pagination.
        Returns the inner list of results (Races, Drivers, etc.)
        """
        all_items = []
        offset = 0

        while True:
            params = {"limit": self.config.page_size, "offset": offset}
            data = self._get(path, params)
            if not data:
                break

            mr = data.get("MRData", {})
            total = int(mr.get("total", 0))
            race_table = mr.get("RaceTable", {})

            # Get whichever list key is present
            races = race_table.get("Races", [])
            all_items.extend(races)

            offset += self.config.page_size
            if offset >= total:
                break

            time.sleep(self.config.request_delay)

        return all_items

    def get_schedule(self, season: int) -> Optional[list]:
        """Full race schedule for a season."""
        data = self._get(f"/{season}/")
        if not data:
            return None
        return data.get("MRData", {}).get("RaceTable", {}).get("Races", [])

    def get_results(self, season: int, round: int) -> Optional[list]:
        """Race results for a specific round."""
        data = self._get(f"/{season}/{round}/results/", {"limit": 30})
        if not data:
            return None
        races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        return races[0].get("Results", []) if races else []

    def get_qualifying(self, season: int, round: int) -> Optional[list]:
        """Qualifying results for a specific round."""
        data = self._get(f"/{season}/{round}/qualifying/", {"limit": 30})
        if not data:
            return None
        races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        return races[0].get("QualifyingResults", []) if races else []

    def get_lap_times(self, season: int, round: int) -> list:
        """
        All lap times for all drivers for a race.
        Paginates through all laps automatically.
        Returns flat list of {lap, driverId, time} dicts.
        """
        all_laps = []
        offset = 0

        while True:
            params = {"limit": self.config.page_size, "offset": offset}
            data = self._get(f"/{season}/{round}/laps/", params)
            if not data:
                break

            mr = data.get("MRData", {})
            total = int(mr.get("total", 0))
            races = mr.get("RaceTable", {}).get("Races", [])

            if races:
                for lap in races[0].get("Laps", []):
                    lap_num = int(lap["number"])
                    for timing in lap.get("Timings", []):
                        all_laps.append({
                            "lap": lap_num,
                            "driverId": timing["driverId"],
                            "time": timing.get("time"),
                            "position": int(timing.get("position", 0)) if timing.get("position") else None,
                        })

            offset += self.config.page_size
            if offset >= total:
                break

            time.sleep(self.config.request_delay)

        return all_laps

    def get_pit_stops(self, season: int, round: int) -> list:
        """All pit stops for a race."""
        all_stops = []
        offset = 0

        while True:
            params = {"limit": self.config.page_size, "offset": offset}
            data = self._get(f"/{season}/{round}/pitstops/", params)
            if not data:
                break

            mr = data.get("MRData", {})
            total = int(mr.get("total", 0))
            races = mr.get("RaceTable", {}).get("Races", [])

            if races:
                all_stops.extend(races[0].get("PitStops", []))

            offset += self.config.page_size
            if offset >= total:
                break

            time.sleep(self.config.request_delay)

        return all_stops

    def get_drivers(self, season: int) -> list:
        """All drivers for a season."""
        data = self._get(f"/{season}/drivers/", {"limit": 50})
        if not data:
            return []
        return data.get("MRData", {}).get("DriverTable", {}).get("Drivers", [])

    def get_constructors(self, season: int) -> list:
        """All constructors for a season."""
        data = self._get(f"/{season}/constructors/", {"limit": 30})
        if not data:
            return []
        return data.get("MRData", {}).get("ConstructorTable", {}).get("Constructors", [])

    def get_race_info(self, season: int, round: int) -> Optional[dict]:
        """Race metadata (name, circuit, date) for a specific round."""
        data = self._get(f"/{season}/{round}/results/", {"limit": 1})
        if not data:
            return None
        races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        if not races:
            return None
        r = races[0]
        circuit = r.get("Circuit", {})
        location = circuit.get("Location", {})
        return {
            "season": int(r["season"]),
            "round": int(r["round"]),
            "race_name": r.get("raceName"),
            "circuit_id": circuit.get("circuitId"),
            "circuit_name": circuit.get("circuitName"),
            "country": location.get("country"),
            "locality": location.get("locality"),
            "race_date": r.get("date"),
            "race_time": r.get("time"),
            "url": r.get("url"),
        }