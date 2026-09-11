"""
transform.py
Transform step of the ETL pipeline.
Reads the raw JSON saved by extract.py, cleans it, and reshapes it into
two structured pandas DataFrames: one for cities (dimension table) and
one for weather readings (fact table).
"""

import json
import glob
import pandas as pd
from datetime import date


def load_latest_raw_file() -> dict:
    """Find and load the most recently saved raw_data JSON file."""
    files = sorted(glob.glob("raw_data/weather_raw_*.json"))
    if not files:
        raise FileNotFoundError(
            "No raw data files found. Run extract.py first."
        )
    latest_file = files[-1]
    print(f"Loading raw data from {latest_file}")
    with open(latest_file, "r") as f:
        return json.load(f)


def transform_to_dataframes(raw_data: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convert raw nested JSON into two clean, structured DataFrames:
      - cities_df: one row per city (dimension table)
      - readings_df: one row per city per day, with current + daily stats (fact table)
    """
    city_rows = []
    reading_rows = []

    for city, payload in raw_data.items():
        # --- Build the cities dimension table ---
        city_rows.append({
            "city": city,
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
            "timezone": payload.get("timezone"),
        })

        # --- Build the readings fact table ---
        current = payload.get("current", {})
        daily = payload.get("daily", {})

        # daily fields come back as lists (one entry per forecast day)
        daily_dates = daily.get("time", [])
        temp_max = daily.get("temperature_2m_max", [])
        temp_min = daily.get("temperature_2m_min", [])
        precip_sum = daily.get("precipitation_sum", [])

        for i, day in enumerate(daily_dates):
            reading_rows.append({
                "city": city,
                "date": day,
                "temp_max_c": temp_max[i] if i < len(temp_max) else None,
                "temp_min_c": temp_min[i] if i < len(temp_min) else None,
                "precipitation_mm": precip_sum[i] if i < len(precip_sum) else None,
                "current_temp_c": current.get("temperature_2m"),
                "current_humidity_pct": current.get("relative_humidity_2m"),
                "current_wind_kmh": current.get("wind_speed_10m"),
            })

    cities_df = pd.DataFrame(city_rows)
    readings_df = pd.DataFrame(reading_rows)

    # --- Cleaning steps ---
    # Drop rows with no temperature data at all (can't do much with them)
    readings_df = readings_df.dropna(subset=["temp_max_c", "temp_min_c"])

    # Add a derived column: daily temperature range - a simple transformation
    # that wouldn't exist in the raw source data
    readings_df["temp_range_c"] = readings_df["temp_max_c"] - readings_df["temp_min_c"]

    # Flag anomalies: unusually hot days (transform logic, not just pass-through)
    readings_df["heat_alert"] = readings_df["temp_max_c"] >= 38

    # Make sure types are correct
    readings_df["date"] = pd.to_datetime(readings_df["date"])

    return cities_df, readings_df


if __name__ == "__main__":
    raw = load_latest_raw_file()
    cities_df, readings_df = transform_to_dataframes(raw)

    print("\n--- Cities ---")
    print(cities_df)

    print("\n--- Weather Readings (first 10 rows) ---")
    print(readings_df.head(10))

    print(f"\nTotal readings: {len(readings_df)}")
    print(f"Heat alerts: {readings_df['heat_alert'].sum()}")