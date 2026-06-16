"""
config.py — Central configuration for City Pulse ETL Pipeline
All paths, constants, and settings live here.
"""

import os

# ── Base paths ─────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, "data")
DAGS_DIR  = os.path.join(BASE_DIR, "dags")
DB_PATH   = os.path.join(DATA_DIR, "city_pulse.db")
LOG_PATH  = os.path.join(DATA_DIR, "pipeline.log")
RAW_DIR   = os.path.join(DATA_DIR, "raw")
PROC_DIR  = os.path.join(DATA_DIR, "processed")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR,  exist_ok=True)
os.makedirs(PROC_DIR, exist_ok=True)

# ── Data sources ───────────────────────────────────────────────────────────────
# NYC TLC Yellow Taxi Trip Data (public Parquet on AWS S3 — no auth needed)
NYC_TAXI_PARQUET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)

# NYC Open Data API (Socrata) — sample endpoint, no key required for small pulls
NYC_OPEN_DATA_BASE = "https://data.cityofnewyork.us/resource"
TAXI_SAMPLE_ENDPOINT = f"{NYC_OPEN_DATA_BASE}/gkne-dk5s.json"

# OpenWeather API (set key via env var or paste in UI)
OPENWEATHER_KEY  = os.environ.get("OPENWEATHER_API_KEY", "")
OPENWEATHER_URL  = "https://api.openweathermap.org/data/2.5/weather"
OPENWEATHER_HIST = "https://api.openweathermap.org/data/2.5/onecall/timemachine"

# ── NYC zones (Manhattan borough_id = 1) ──────────────────────────────────────
MANHATTAN_ZONE_IDS = list(range(1, 70))   # simplified mapping

NYC_NEIGHBORHOODS = [
    "Midtown", "Times Square", "Upper East Side", "Upper West Side",
    "Lower East Side", "Greenwich Village", "Chelsea", "Harlem",
    "Financial District", "Hell's Kitchen", "Tribeca", "SoHo",
    "East Village", "Murray Hill", "Gramercy", "Astoria",
    "Williamsburg", "Park Slope", "Flushing", "Jamaica",
    "Bronx Center", "Staten Island", "JFK Airport", "LaGuardia Airport",
]

# ── Pipeline schedule ──────────────────────────────────────────────────────────
SCHEDULE_INTERVAL_MIN = 60       # run every 60 minutes
TAXI_ROWS_PER_RUN     = 50_000   # synthetic rows per ETL run
WEATHER_CITIES        = ["New York", "Manhattan", "Brooklyn"]

# ── Data quality thresholds ────────────────────────────────────────────────────
MAX_TRIP_DISTANCE_MILES = 100
MAX_FARE_AMOUNT         = 500
MAX_TRIP_DURATION_MIN   = 180
MIN_PASSENGER_COUNT     = 1
MAX_PASSENGER_COUNT     = 6

# ── Dashboard ──────────────────────────────────────────────────────────────────
DASHBOARD_PORT = 5050
DASHBOARD_HOST = "0.0.0.0"

# ── Airflow-like settings ──────────────────────────────────────────────────────
AIRFLOW_HOME = os.path.join(BASE_DIR, "airflow_home")
os.makedirs(AIRFLOW_HOME, exist_ok=True)
