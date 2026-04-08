"""
NASCAR CDN API Client

Native replacement for pynascar. Calls cf.nascar.com directly.
No third-party dependency, no auth required.
"""

import requests
from dataclasses import dataclass
from typing import Optional, Any, Dict


@dataclass
class NASCARConfig:
    base_url: str = "https://cf.nascar.com/cacher"
    live_url: str = "https://cf.nascar.com/cacher/live"
    loop_url: str = "https://cf.nascar.com/loopstats/prod"
    timeout: int = 60


class NASCARClient:
    """
    Thin client for the NASCAR CDN API.
    All methods return raw parsed JSON (dict or list).
    Transform logic lives in transform.py, not here.
    """

    def __init__(self, config: NASCARConfig = NASCARConfig()):
        self.config = config

    def _get(self, url: str) -> Optional[Any]:
        try:
            response = requests.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"  [NASCARClient] Failed: {url}\n  Error: {e}")
            return None

    def get_schedule(self, year: int) -> Optional[Any]:
        """Full race schedule for a season. Use this to discover race_ids."""
        url = f"{self.config.base_url}/{year}/race_list_basic.json"
        return self._get(url)

    def get_weekend_feed(self, year: int, series_id: int, race_id: int) -> Optional[Dict]:
        """Race weekend data: results, cautions, lead changes, stage results, practice."""
        url = f"{self.config.base_url}/{year}/{series_id}/{race_id}/weekend-feed.json"
        return self._get(url)

    def get_lap_times(self, year: int, series_id: int, race_id: int) -> Optional[Any]:
        """Lap-by-lap times for all drivers. Most granular data available."""
        url = f"{self.config.base_url}/{year}/{series_id}/{race_id}/lap-times.json"
        return self._get(url)

    def get_pit_stops(self, year: int, series_id: int, race_id: int) -> Optional[Any]:
        """Pit stop data including duration and lap."""
        url = f"{self.config.base_url}/{year}/{series_id}/{race_id}/live-pit-data.json"
        return self._get(url)

    def get_lap_notes(self, year: int, series_id: int, race_id: int) -> Optional[Any]:
        """Lap-by-lap event notes (caution details, incidents, etc.)."""
        url = f"{self.config.base_url}/{year}/{series_id}/{race_id}/lap-notes.json"
        return self._get(url)

    def get_driver_stats(self, year: int, series_id: int, race_id: int) -> Optional[Any]:
        """Loop stats: driver rating, quality passes, avg running position, etc."""
        url = f"{self.config.loop_url}/{year}/{series_id}/{race_id}.json"
        return self._get(url)

    def get_advanced_stats(self, series_id: int, race_id: int) -> Optional[Any]:
        """Advanced live feed. Most reliable for recent/current races."""
        url = f"{self.config.live_url}/series_{series_id}/{race_id}/live-feed.json"
        return self._get(url)

    def get_all_race_data(self, year: int, series_id: int, race_id: int) -> Dict[str, Any]:
        """Fetch all endpoints for a race in one call. None if unavailable."""
        print(f"Fetching all data for {year} series={series_id} race={race_id}...")
        return {
            "weekend_feed":   self.get_weekend_feed(year, series_id, race_id),
            "lap_times":      self.get_lap_times(year, series_id, race_id),
            "pit_stops":      self.get_pit_stops(year, series_id, race_id),
            "lap_notes":      self.get_lap_notes(year, series_id, race_id),
            "driver_stats":   self.get_driver_stats(year, series_id, race_id),
            "advanced_stats": self.get_advanced_stats(series_id, race_id),
        }