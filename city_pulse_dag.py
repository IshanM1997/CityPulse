"""
dags/city_pulse_dag.py — Airflow DAG definitions for City Pulse ETL
=====================================================================
Two DAGs:
  1. city_pulse_hourly   — runs every hour; ingests ~50k trip records
  2. city_pulse_daily    — runs at midnight; computes daily rollups
  3. city_pulse_backfill — one-shot; generates 90 days of history

These are valid Apache Airflow DAGs. They also expose a run() function
so they can be triggered from our custom scheduler without Airflow installed.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid
import json
from datetime import datetime, timedelta
from config import TAXI_ROWS_PER_RUN, WEATHER_CITIES, SCHEDULE_INTERVAL_MIN

# ── Try to import Airflow; degrade gracefully if not installed ─────────────────
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("[DAG] Apache Airflow not installed — DAGs run via built-in scheduler")


# ══════════════════════════════════════════════════════════════════════════════
#  TASK FUNCTIONS
#  Each function is one Airflow task. They are also callable standalone.
# ══════════════════════════════════════════════════════════════════════════════

def task_extract_taxi(n_rows: int = TAXI_ROWS_PER_RUN,
                      span_days: int = 7, **kwargs) -> dict:
    """
    EXTRACT — Pull NYC taxi trip data.
    Tries real API first; falls back to synthetic data.
    """
    from data_generator import fetch_real_taxi_sample, generate_batch
    import database as db

    run_id  = kwargs.get("run_id", str(uuid.uuid4())[:12])
    dag_id  = kwargs.get("dag_id", "city_pulse_hourly")
    task_id = "extract_taxi"

    print(f"\n[Task:{task_id}] Extracting {n_rows} trip records …")
    db.insert_pipeline_run(run_id, dag_id, task_id, "running")

    # Try real data first
    real_trips = fetch_real_taxi_sample(min(n_rows, 500))
    if real_trips:
        print(f"[Task:{task_id}] Real API: got {len(real_trips)} records")
        # Top up with synthetic if needed
        if len(real_trips) < n_rows:
            synthetic = generate_batch(n_rows - len(real_trips), span_days=span_days)
            raw_trips = real_trips + synthetic
        else:
            raw_trips = real_trips[:n_rows]
    else:
        print(f"[Task:{task_id}] Using synthetic data ({n_rows} records)")
        raw_trips = generate_batch(n_rows, span_days=span_days)

    print(f"[Task:{task_id}] ✓ Extracted {len(raw_trips)} trips")
    db.update_pipeline_run(run_id, "running", rows_extracted=len(raw_trips))

    # Airflow XCom push
    if kwargs.get("ti"):
        kwargs["ti"].xcom_push(key="raw_trips", value=raw_trips[:1000])  # XCom limit
    return {"run_id": run_id, "count": len(raw_trips), "trips": raw_trips}


def task_extract_weather(**kwargs) -> dict:
    """
    EXTRACT — Pull current weather for NYC.
    Tries OpenWeather API first; falls back to synthetic.
    """
    from data_generator import fetch_real_weather, generate_weather_reading
    from datetime import datetime, timedelta
    import database as db

    run_id  = kwargs.get("run_id", str(uuid.uuid4())[:12])
    dag_id  = kwargs.get("dag_id", "city_pulse_hourly")
    task_id = "extract_weather"

    print(f"\n[Task:{task_id}] Fetching weather data …")
    weather_rows = []

    for city in WEATHER_CITIES:
        real = fetch_real_weather(city)
        if real:
            weather_rows.append(real)
        else:
            # Generate readings for last 24 hours
            for h in range(24):
                dt = datetime.now() - timedelta(hours=h)
                weather_rows.append(generate_weather_reading(city, dt))

    print(f"[Task:{task_id}] ✓ {len(weather_rows)} weather readings")
    return {"run_id": run_id, "weather": weather_rows}


def task_transform_load(extract_result: dict = None,
                        weather_result: dict = None,
                        **kwargs) -> dict:
    """
    TRANSFORM + LOAD — Clean, validate, aggregate, store.
    """
    from transforms import run_etl
    import database as db

    run_id = (extract_result or {}).get("run_id", str(uuid.uuid4())[:12])
    dag_id = kwargs.get("dag_id", "city_pulse_hourly")

    print(f"\n[Task:transform_load] run_id={run_id}")
    db.insert_pipeline_run(run_id, dag_id, "transform_load", "running")

    raw_trips   = (extract_result or {}).get("trips", [])
    raw_weather = (weather_result or {}).get("weather", [])

    try:
        stats = run_etl(raw_trips, raw_weather, run_id=run_id, dag_id=dag_id)
        db.update_pipeline_run(
            run_id, "success",
            rows_extracted=stats["raw_trips"],
            rows_loaded=stats["stg_trips_loaded"],
            rows_rejected=stats["raw_trips"] - stats["stg_trips_loaded"],
            metadata=stats,
        )
        print(f"[Task:transform_load] ✓ Success")
        return stats
    except Exception as e:
        db.update_pipeline_run(run_id, "failed", error_message=str(e))
        raise


def task_daily_rollup(**kwargs) -> dict:
    """
    Run at midnight — compute weekly trends and data quality report.
    """
    import database as db
    from datetime import date

    run_id  = str(uuid.uuid4())[:12]
    today   = date.today().isoformat()
    print(f"\n[Task:daily_rollup] date={today}")
    db.insert_pipeline_run(run_id, "city_pulse_daily", "daily_rollup", "running")

    try:
        with db.get_conn() as conn:
            # Compute weekly trends
            week_start = (datetime.now() - timedelta(days=7)).date().isoformat()
            conn.execute("""
                INSERT INTO agg_weekly_trends
                  (week_start, week_end, total_trips, total_revenue,
                   avg_daily_trips, peak_day, peak_hour, computed_at)
                SELECT
                  ? as week_start, ? as week_end,
                  SUM(total_trips), SUM(total_revenue),
                  AVG(total_trips),
                  (SELECT date FROM agg_daily_summary
                   WHERE date >= ? ORDER BY total_trips DESC LIMIT 1),
                  (SELECT hour FROM agg_hourly_pulse
                   WHERE date >= ? ORDER BY trip_count DESC LIMIT 1),
                  ?
                FROM agg_daily_summary WHERE date >= ?
                ON CONFLICT(week_start) DO UPDATE SET
                  total_trips=excluded.total_trips,
                  total_revenue=excluded.total_revenue,
                  computed_at=excluded.computed_at
            """, (week_start, today, week_start, week_start,
                  datetime.utcnow().isoformat(), week_start))

            # Weather impact join
            conn.execute("""
                INSERT OR REPLACE INTO agg_weather_impact
                  (date, hour, trip_count, avg_fare, temp_f, condition,
                   is_rain, is_snow, wind_speed_mph, computed_at)
                SELECT
                  p.date, p.hour, p.trip_count, p.avg_fare,
                  w.temp_f, w.condition, w.is_rain, w.is_snow, w.wind_speed_mph,
                  ? as computed_at
                FROM agg_hourly_pulse p
                LEFT JOIN stg_weather w
                  ON substr(w.recorded_at,1,10) = p.date
                  AND CAST(strftime('%H', w.recorded_at) AS INTEGER) = p.hour
                WHERE p.date >= ?
            """, (datetime.utcnow().isoformat(), week_start))

        db.update_pipeline_run(run_id, "success")
        print(f"[Task:daily_rollup] ✓ Weekly trends & weather impact updated")
        return {"run_id": run_id, "status": "success"}
    except Exception as e:
        db.update_pipeline_run(run_id, "failed", error_message=str(e))
        raise


def task_data_quality_report(**kwargs) -> dict:
    """
    Run DQ checks on the warehouse and generate a report.
    """
    import database as db

    run_id  = str(uuid.uuid4())[:12]
    print(f"\n[Task:dq_report] run_id={run_id}")
    db.insert_pipeline_run(run_id, "city_pulse_daily", "dq_report", "running")

    with db.get_conn() as conn:
        counts = db.get_data_counts()

        # Check: stg_trips has data
        db.log_dq_check(run_id, "stg_trips_not_empty", "stg_taxi_trips",
                        "PASS" if counts["stg_trips"] > 0 else "FAIL",
                        ">0", counts["stg_trips"])

        # Check: hourly pulse covers multiple hours
        hours_covered = conn.execute(
            "SELECT COUNT(DISTINCT hour) FROM agg_hourly_pulse"
        ).fetchone()[0]
        db.log_dq_check(run_id, "hourly_coverage", "agg_hourly_pulse",
                        "PASS" if hours_covered >= 20 else "WARN",
                        "≥20 hours", hours_covered)

        # Check: no negative fares
        neg_fares = conn.execute(
            "SELECT COUNT(*) FROM stg_taxi_trips WHERE fare_amount < 0"
        ).fetchone()[0]
        db.log_dq_check(run_id, "no_negative_fares", "stg_taxi_trips",
                        "PASS" if neg_fares == 0 else "FAIL", 0, neg_fares)

        # Check: neighborhoods not blank
        blank_hoods = conn.execute(
            "SELECT COUNT(*) FROM stg_taxi_trips WHERE pickup_neighborhood IS NULL"
        ).fetchone()[0]
        db.log_dq_check(run_id, "no_null_neighborhoods", "stg_taxi_trips",
                        "PASS" if blank_hoods == 0 else "WARN", 0, blank_hoods)

        # Check: pulse scores reasonable
        bad_pulse = conn.execute(
            "SELECT COUNT(*) FROM agg_hourly_pulse WHERE pulse_score < 0 OR pulse_score > 100"
        ).fetchone()[0]
        db.log_dq_check(run_id, "pulse_score_range", "agg_hourly_pulse",
                        "PASS" if bad_pulse == 0 else "FAIL", "0-100", bad_pulse)

    db.update_pipeline_run(run_id, "success", metadata=counts)
    print(f"[Task:dq_report] ✓ {len(db.get_dq_summary(run_id))} checks logged")
    return {"run_id": run_id, "counts": counts}


# ══════════════════════════════════════════════════════════════════════════════
#  AIRFLOW DAG DEFINITIONS (used when Airflow is installed)
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    "owner":            "city_pulse",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

if AIRFLOW_AVAILABLE:

    # ── DAG 1: Hourly ingestion ────────────────────────────────────────────────
    with DAG(
        dag_id="city_pulse_hourly",
        description="Ingest NYC taxi + weather data every hour",
        default_args=DEFAULT_ARGS,
        start_date=days_ago(1),
        schedule_interval=f"0 * * * *",   # top of every hour
        catchup=False,
        tags=["city_pulse", "etl", "nyc"],
    ) as hourly_dag:

        extract_taxi_task = PythonOperator(
            task_id="extract_taxi",
            python_callable=task_extract_taxi,
            op_kwargs={"n_rows": TAXI_ROWS_PER_RUN, "span_days": 1},
        )

        extract_weather_task = PythonOperator(
            task_id="extract_weather",
            python_callable=task_extract_weather,
        )

        def _transform_load_airflow(**kwargs):
            ti           = kwargs["ti"]
            taxi_result  = ti.xcom_pull(task_ids="extract_taxi")
            wx_result    = ti.xcom_pull(task_ids="extract_weather")
            return task_transform_load(taxi_result, wx_result, **kwargs)

        transform_load_task = PythonOperator(
            task_id="transform_load",
            python_callable=_transform_load_airflow,
        )

        [extract_taxi_task, extract_weather_task] >> transform_load_task

    # ── DAG 2: Daily rollup ────────────────────────────────────────────────────
    with DAG(
        dag_id="city_pulse_daily",
        description="Daily rollup — weekly trends, DQ checks, weather impact",
        default_args=DEFAULT_ARGS,
        start_date=days_ago(1),
        schedule_interval="0 0 * * *",   # midnight
        catchup=False,
        tags=["city_pulse", "analytics", "dq"],
    ) as daily_dag:

        rollup_task = PythonOperator(
            task_id="daily_rollup",
            python_callable=task_daily_rollup,
        )
        dq_task = PythonOperator(
            task_id="dq_report",
            python_callable=task_data_quality_report,
        )
        rollup_task >> dq_task

    # ── DAG 3: Backfill ────────────────────────────────────────────────────────
    with DAG(
        dag_id="city_pulse_backfill",
        description="One-time 90-day backfill — generates historical data",
        default_args=DEFAULT_ARGS,
        start_date=days_ago(1),
        schedule_interval=None,   # manual trigger only
        catchup=False,
        tags=["city_pulse", "backfill"],
    ) as backfill_dag:

        def _backfill(**kwargs):
            from data_generator import generate_batch, generate_weather_reading
            from datetime import datetime, timedelta
            raw_trips = generate_batch(n=300_000, span_days=90)
            raw_weather = []
            for d in range(90):
                dt = datetime.now() - timedelta(days=d)
                for h in range(0, 24, 3):
                    raw_weather.append(generate_weather_reading("New York",
                        dt.replace(hour=h)))
            return task_transform_load(
                {"run_id": "backfill", "trips": raw_trips},
                {"weather": raw_weather},
                dag_id="city_pulse_backfill"
            )

        backfill_task = PythonOperator(
            task_id="backfill_90d",
            python_callable=_backfill,
        )
