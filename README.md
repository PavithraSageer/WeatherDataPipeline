# WeatherDataPipeline

An end-to-end ETL (Extract, Transform, Load) pipeline built in Python that collects weather data for multiple Indian cities, transforms and cleans the data using Pandas, and loads it into a normalized SQLite database.

## Pipeline

### 1. Extract : `extract.py`

Fetches current weather and 7-day forecast data from the free [Open-Meteo API](https://open-meteo.com/) for five Indian cities.

The raw API responses are saved as JSON files in `raw_data/`, allowing the Transform step to be re-run without making another API request.

### 2. Transform : `transform.py`

Reads the raw JSON data and converts it into two structured Pandas DataFrames:

* **cities** — city information such as latitude, longitude, and timezone
* **readings** — daily weather measurements for each city

Transformation and cleaning steps include:

* Handling missing temperature values
* Converting dates into a standard format
* Calculating daily temperature range
* Generating a `heat_alert` flag for maximum temperatures ≥ 38°C

### 3. Load : `load.py`

Loads the transformed data into a SQLite database named `weather_data.db`.

The database contains two related tables:

* `cities`
* `readings`

The tables are linked using a foreign key. A unique constraint on `(city, date)` combined with `INSERT OR REPLACE` makes the loading process **idempotent**, allowing the pipeline to be safely re-run without creating duplicate records.

## Architecture

```text
Open-Meteo API
      ↓
   Extract
      ↓
   Raw JSON
      ↓
  Transform
      ↓
 Pandas DataFrames
      ↓
     Load
      ↓
 SQLite Database
      ↓
  SQL Queries
```

## Tech Stack

* Python
* Requests
* Pandas
* SQLite
* SQL
* REST API

## Database

The pipeline creates `weather_data.db` containing:

### `cities`

| Column    | Type               |
| --------- | ------------------ |
| city      | TEXT (Primary Key) |
| latitude  | REAL               |
| longitude | REAL               |
| timezone  | TEXT               |

### `readings`

| Column               | Type                  |
| -------------------- | --------------------- |
| id                   | INTEGER (Primary Key) |
| city                 | TEXT (Foreign Key)    |
| date                 | TEXT                  |
| temp_max_c           | REAL                  |
| temp_min_c           | REAL                  |
| precipitation_mm     | REAL                  |
| current_temp_c       | REAL                  |
| current_humidity_pct | REAL                  |
| current_wind_kmh     | REAL                  |
| temp_range_c         | REAL                  |
| heat_alert           | INTEGER               |

## How to Run

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Run the complete pipeline:

```bash
python load.py
```

This executes:

```text
Extract → Transform → Load
```

and creates `weather_data.db`.

## Example SQL Query

The database can be queried using Python and SQLite:

```python
import sqlite3

conn = sqlite3.connect("weather_data.db")

result = conn.execute("""
    SELECT city,
           ROUND(AVG(temp_max_c), 2) AS avg_max_temp
    FROM readings
    GROUP BY city
    ORDER BY avg_max_temp DESC
""").fetchall()

print(result)

conn.close()
```

Example output:

```text
Chennai    32.47
Delhi      30.97
Bengaluru  30.54
Kochi      29.83
Mumbai     28.76
```

## Future Improvements

* Schedule automated daily pipeline runs using cron or Airflow
* Add incremental data loading
* Add logging and error monitoring
* Add more cities
* Build a Streamlit dashboard for data visualization
