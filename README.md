# 🏙 City Pulse — NYC ETL Pipeline

> **Which hour is Manhattan most alive?**
> A production-grade local ETL pipeline with Airflow DAGs, Pandas transforms, SQLite warehouse, and an interactive Plotly dashboard.

## Project Structure

```
city_pulse/
├── start.sh                ← One-command launcher (Mac/Linux)
├── start.bat               ← One-command launcher (Windows)
├── run.py                  ← Python launcher (used by startup scripts)
├── dashboard.py            ← Flask dashboard server
├── scheduler.py            ← Built-in DAG scheduler (no Airflow needed)
├── config.py               ← All settings & constants
├── database.py             ← SQLite warehouse (raw → staging → analytics)
├── transforms.py           ← Full pandas ETL pipeline + DQ checks
├── data_generator.py       ← Synthetic NYC taxi + weather data
├── requirements.txt
├── README.md
├── dags/
│   ├── __init__.py         ← Required for Python imports (auto-created by scripts)
│   └── city_pulse_dag.py   ← Airflow DAG definitions (3 DAGs)
└── data/                   ← Auto-created on first run
    ├── city_pulse.db        ← SQLite warehouse
    ├── raw/                 ← Raw data staging
    └── processed/           ← Processed output
```

## Quick Start

The easiest way to run the project is with the provided startup scripts.
They handle everything automatically — virtual environment, dependencies, and launch.

**Mac / Linux — `start.sh`**
```bash
# Place start.sh in the city_pulse/ folder, then:
bash start.sh
```

**Windows — `start.bat`**
```
Double-click start.bat
```

That's it. Both scripts will:
1. Check Python 3.10+ is installed
2. Create `dags/__init__.py` if missing
3. Verify all required project files are present
4. Create and activate a virtual environment
5. Install all dependencies
6. Launch the pipeline with 90 days of backfilled data

Once running, open the dashboard at **http://localhost:5050**. Press `Ctrl+C` to stop.

---

### Manual Quick Start (without the scripts)

```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

# 2. Install dependencies
pip install flask pandas requests numpy

# 3. Create the dags package init file (required)
touch dags/__init__.py          # Mac/Linux
echo. > dags\__init__.py        # Windows

# 4. Launch everything (scheduler + dashboard)
python run.py --backfill

# 5. Open dashboard
# http://localhost:5050
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

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: No module named 'dags'` | Run `touch dags/__init__.py` (Mac/Linux) or `echo. > dags\__init__.py` (Windows) |
| `ModuleNotFoundError: No module named 'dashboard'` | Make sure the file is named `dashboard.py`, not `app.py` |
| Port 5050 already in use | Change `DASHBOARD_PORT = 5050` in `config.py` to another port e.g. `5051` |
| Database errors on re-run | Delete the `data/` folder and re-run — `init_db()` recreates everything |
| Blank dashboard / no charts | Run with `--backfill` first to seed historical data |

## VS Code Setup

```bash
python -m venv venv
venv\Scripts\activate           # Windows
source venv/bin/activate        # Mac/Linux

pip install flask pandas requests numpy

touch dags/__init__.py          # Mac/Linux
echo. > dags\__init__.py        # Windows

python run.py --backfill
```
