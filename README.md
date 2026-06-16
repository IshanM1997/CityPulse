# 🏙 City Pulse — NYC ETL Pipeline

> **Which hour is Manhattan most alive?**
> A production-grade local ETL pipeline with Airflow DAGs, Pandas transforms, SQLite warehouse, and an interactive Plotly dashboard.

## Project Structure (3 folders, no sub-folders)

```
city_pulse/
├── run.py                  ← One-command launcher (start everything)
├── app.py → dashboard.py   ← Flask dashboard server
├── scheduler.py            ← Built-in DAG scheduler (no Airflow needed)
├── config.py               ← All settings & constants
├── database.py             ← SQLite warehouse (raw → staging → analytics)
├── transforms.py           ← Full pandas ETL pipeline + DQ checks
├── data_generator.py       ← Synthetic NYC taxi + weather data
├── requirements.txt
├── README.md
├── dags/
│   └── city_pulse_dag.py   ← Airflow DAG definitions (3 DAGs)
└── data/
    ├── city_pulse.db        ← SQLite warehouse (auto-created)
    ├── raw/                 ← Raw data staging
    └── processed/           ← Processed output
```

## Quick Start

```bash
# 1. Install
pip install flask pandas requests

# 2. Launch everything (scheduler + dashboard)
python run.py

# 3. Open dashboard
http://localhost:5050

# 4. Click "Backfill 90 Days" for full history, or
#    Click "Run ETL (10k)" for a quick test
```

## Commands

```bash
# Start everything
python run.py

# Seed 90 days of historical data first (recommended for first run)
python run.py --backfill

# Dashboard only (no auto-ETL)
python run.py --dashboard-only

# ETL only (headless)
python run.py --etl-only

# Run pipeline once and show status
python scheduler.py --once --status

# Show pipeline status
python scheduler.py --status

# Backfill with custom row count
python run.py --backfill --rows 500000
```

## With Apache Airflow

```bash
# Install Airflow (optional)
pip install apache-airflow

# Initialize Airflow DB
airflow db init

# Copy DAGs
export AIRFLOW_HOME=~/airflow
cp dags/city_pulse_dag.py ~/airflow/dags/

# Start Airflow webserver + scheduler
airflow webserver -p 8080 &
airflow scheduler &

# Open Airflow UI
http://localhost:8080
# DAGs: city_pulse_hourly, city_pulse_daily, city_pulse_backfill
```

## Data Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     AIRFLOW / SCHEDULER                         │
│  city_pulse_hourly (*/1 * * * *)                               │
│    ├─ task_extract_taxi    ← NYC TLC API / Synthetic fallback  │
│    ├─ task_extract_weather ← OpenWeather API / Synthetic       │
│    └─ task_transform_load  ← Pandas + DQ checks + SQLite       │
│                                                                 │
│  city_pulse_daily (0 0 * * *)                                  │
│    ├─ task_daily_rollup    ← Weekly trends, weather impact     │
│    └─ task_dq_report       ← Data quality validation          │
│                                                                 │
│  city_pulse_backfill (manual)                                   │
│    └─ backfill_90d         ← Historical data generation        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     SQLite WAREHOUSE                            │
│  RAW LAYER         │ STAGING LAYER      │ ANALYTICS LAYER      │
│  raw_taxi_trips    │ stg_taxi_trips     │ agg_hourly_pulse     │
│  raw_weather       │ stg_weather        │ agg_neighborhood_stats│
│                    │                    │ agg_daily_summary    │
│                    │                    │ agg_weekly_trends    │
│                    │                    │ agg_weather_impact   │
│                    │                    │ pipeline_runs        │
│                    │                    │ data_quality_log     │
└─────────────────────────────────────────────────────────────────┘
```

## Dashboard Sections

| Section | What it answers |
|---------|----------------|
| **Pulse Hero** | Which hour is Manhattan most alive? (top 10 hours ranked) |
| **Heatmap** | Hour × Weekday activity grid — spot rush hours visually |
| **Hourly Average** | City rhythm — when do cabs surge? |
| **Daily Trend** | 30-day trip volume trend |
| **Neighborhoods** | Where does Manhattan move most? |
| **Weekday vs Weekend** | How does Saturday differ from Monday? |
| **Pulse Score** | Composite city-alive score by hour |
| **Weather Impact** | Do rainy days change trip counts? |
| **Fare by Hour** | When are fares highest? |
| **Pipeline Log** | Real-time ETL run history |
| **DQ Checks** | Data quality pass/fail/warn log |
| **Recent Trips** | Live table of latest 50 processed records |

## Data Sources

| Source | URL | Auth |
|--------|-----|------|
| NYC TLC Taxi Data | Socrata API / AWS Parquet | None (public) |
| OpenWeather | api.openweathermap.org | Free API key |
| Synthetic fallback | Built-in generator | None |

Set `OPENWEATHER_API_KEY` env var for real weather data.
The pipeline always works with synthetic data if APIs are unreachable.

## VS Code Setup

```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

pip install flask pandas requests
python run.py
```
