"""
load.py
Load step of the ETL pipeline.
Takes the cleaned DataFrames from transform.py and writes them into a
normalized SQLite database with a proper schema (cities + readings,
linked by a foreign key). Designed to be safe to re-run (idempotent) -
running it daily won't create duplicate rows.
"""

import sqlite3
import pandas as pd

DB_PATH = "weather_data.db"


def create_schema(conn: sqlite3.Connection) -> None:
    """Create the cities (dimension) and readings (fact) tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cities (
            city TEXT PRIMARY KEY,
            latitude REAL,
            longitude REAL,
            timezone TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            date TEXT NOT NULL,
            temp_max_c REAL,
            temp_min_c REAL,
            precipitation_mm REAL,
            current_temp_c REAL,
            current_humidity_pct REAL,
            current_wind_kmh REAL,
            temp_range_c REAL,
            heat_alert INTEGER,
            FOREIGN KEY (city) REFERENCES cities (city),
            UNIQUE (city, date)
        )
    """)
    conn.commit()


def load_cities(conn: sqlite3.Connection, cities_df: pd.DataFrame) -> None:
    """Upsert city dimension data. Cities rarely change, so INSERT OR REPLACE is fine."""
    for _, row in cities_df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO cities (city, latitude, longitude, timezone)
            VALUES (?, ?, ?, ?)
        """, (row["city"], row["latitude"], row["longitude"], row["timezone"]))
    conn.commit()
    print(f"Loaded {len(cities_df)} rows into cities")


def load_readings(conn: sqlite3.Connection, readings_df: pd.DataFrame) -> None:
    """
    Upsert weather readings. Uses INSERT OR REPLACE on the (city, date) unique
    constraint so re-running the pipeline on the same day updates the row
    instead of creating a duplicate - this makes the pipeline idempotent,
    a key property of a well-designed ETL job.
    """
    rows_loaded = 0
    for _, row in readings_df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO readings
                (city, date, temp_max_c, temp_min_c, precipitation_mm,
                 current_temp_c, current_humidity_pct, current_wind_kmh,
                 temp_range_c, heat_alert)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["city"],
            row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else row["date"],
            row["temp_max_c"],
            row["temp_min_c"],
            row["precipitation_mm"],
            row["current_temp_c"],
            row["current_humidity_pct"],
            row["current_wind_kmh"],
            row["temp_range_c"],
            int(row["heat_alert"]),
        ))
        rows_loaded += 1
    conn.commit()
    print(f"Loaded {rows_loaded} rows into readings")


def load(cities_df: pd.DataFrame, readings_df: pd.DataFrame) -> None:
    """Main load entry point: opens the DB connection, creates schema, loads both tables."""
    conn = sqlite3.connect(DB_PATH)
    try:
        create_schema(conn)
        load_cities(conn, cities_df)
        load_readings(conn, readings_df)
    finally:
        conn.close()
    print(f"\nData written to {DB_PATH}")


if __name__ == "__main__":
    # Run the full pipeline end-to-end: Extract -> Transform -> Load
    from extract import extract_all_cities
    from transform import transform_to_dataframes

    print("=== EXTRACT ===")
    raw_data = extract_all_cities()

    print("\n=== TRANSFORM ===")
    cities_df, readings_df = transform_to_dataframes(raw_data)

    print("\n=== LOAD ===")
    load(cities_df, readings_df)

    print("\nPipeline complete! Query weather_data.db to see your results.")