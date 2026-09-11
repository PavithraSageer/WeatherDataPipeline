"""
extract.py
Extract step of the ETL pipeline.
Pulls current + daily weather data for multiple cities from the Open-Meteo API
(free, no API key required) and saves the raw JSON responses to disk.
"""

import requests
import json
import os
from datetime import date

# City name -> (latitude, longitude)
CITIES = {
    "Kochi": (9.9312, 76.2673),
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
}

RAW_DATA_DIR = "raw_data"


def fetch_weather(city: str, lat: float, lon: float) -> dict:
    """Call the Open-Meteo API for a single city and return the raw JSON response."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "auto",
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()  # raise an error if the API call failed
    return response.json()


def extract_all_cities() -> dict:
    """Fetch weather data for every city in CITIES and return a dict keyed by city name."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    all_data = {}

    for city, (lat, lon) in CITIES.items():
        print(f"Extracting weather data for {city}...")
        try:
            data = fetch_weather(city, lat, lon)
            all_data[city] = data
        except requests.exceptions.RequestException as e:
            print(f"  Failed to fetch data for {city}: {e}")

    # Save raw JSON to disk, timestamped by today's date.
    # Keeping raw extracted data is a real ETL best practice - it lets you
    # re-run the Transform step later without hitting the API again.
    filename = f"{RAW_DATA_DIR}/weather_raw_{date.today().isoformat()}.json"
    with open(filename, "w") as f:
        json.dump(all_data, f, indent=2)

    print(f"\nSaved raw data for {len(all_data)} cities to {filename}")
    return all_data


if __name__ == "__main__":
    extract_all_cities()