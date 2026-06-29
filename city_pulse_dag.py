"""
dags/city_pulse_dag.py — Airflow DAG definitions for City Pulse ETL
====================================================================
All task functions accept a `city` parameter so any supported city
from CITY_PROFILES can be processed.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid
from datetime import datetime, timedelta
from config import TAXI_ROWS_PER_RUN, SCHEDULE_INTERVAL_MIN, DEFAULT_CITY

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
# ══════════════════════════════════════════════════════════════════════════════

def task_extract_taxi(n_rows=TAXI_ROWS_PER_RUN, span_days=7,
                      city=DEFAULT_CITY, **kwargs):
    """EXTRACT — Pull taxi trip data for the given city."""
    from data_generator import fetch_real_taxi_sample, generate_batch
    import database as db

    run_id  = kwargs.get("run_id", str(uuid.uuid4())[:12])
    dag_id  = kwargs.get("dag_id", "city_pulse_hourly")
    task_id = "extract_taxi"

    print(f"\n[Task:{task_id}] City: {city} — Extracting {n_rows} trips …")
    db.insert_pipeline_run(run_id, dag_id, task_id, "running", city=city)

    # Real data only available for New York
    raw_trips = []
    if city == "New York":
        raw_trips = fetch_real_taxi_sample(min(n_rows, 500))
        if raw_trips:
            print(f"[Task:{task_id}] Real API: got {len(raw_trips)} records")
            if len(raw_trips) < n_rows:
                raw_trips += generate_batch(n_rows - len(raw_trips), city=city, span_days=span_days)

    if not raw_trips:
        print(f"[Task:{task_id}] Using synthetic data ({n_rows} records)")
        raw_trips = generate_batch(n_rows, city=city, span_days=span_days)

    print(f"[Task:{task_id}] Extracted {len(raw_trips)} trips")
    db.update_pipeline_run(run_id, "running", rows_extracted=len(raw_trips))
    return {"run_id": run_id, "city": city, "count": len(raw_trips), "trips": raw_trips}


def task_extract_weather(city=DEFAULT_CITY, **kwargs):
    """EXTRACT — Pull weather data for the given city."""
    from data_generator import fetch_real_weather, generate_weather_reading

    run_id  = kwargs.get("run_id", str(uuid.uuid4())[:12])
    dag_id  = kwargs.get("dag_id", "city_pulse_hourly")
    task_id = "extract_weather"

    print(f"\n[Task:{task_id}] City: {city} — Fetching weather …")
    weather_rows = []

    real = fetch_real_weather(city)
    if real:
        weather_rows.append(real)
    else:
        for h in range(24):
            dt = datetime.now() - timedelta(hours=h)
            weather_rows.append(generate_weather_reading(city, dt))

    print(f"[Task:{task_id}] {len(weather_rows)} weather readings")
    return {"run_id": run_id, "city": city, "weather": weather_rows}


def task_transform_load(extract_result=None, weather_result=None,
                        city=DEFAULT_CITY, **kwargs):
    """TRANSFORM + LOAD — Clean, validate, aggregate, store."""
    from transforms import run_etl
    import database as db

    run_id = (extract_result or {}).get("run_id", str(uuid.uuid4())[:12])
    city   = (extract_result or {}).get("city", city)
    dag_id = kwargs.get("dag_id", "city_pulse_hourly")

    print(f"\n[Task:transform_load] City: {city}  run_id={run_id}")
    db.insert_pipeline_run(run_id, dag_id, "transform_load", "running", city=city)

    raw_trips   = (extract_result or {}).get("trips",   [])
    raw_weather = (weather_result or {}).get("weather", [])

    try:
        stats = run_etl(raw_trips, raw_weather, city=city,
                        run_id=run_id, dag_id=dag_id)
        db.update_pipeline_run(
            run_id, "success",
            rows_extracted=stats["raw_trips"],
            rows_loaded=stats["stg_trips_loaded"],
            rows_rejected=stats["raw_trips"] - stats["stg_trips_loaded"],
            metadata=stats,
        )
        print(f"[Task:transform_load] Success")
        return stats
    except Exception as e:
        db.update_pipeline_run(run_id, "failed", error_message=str(e))
        raise


def task_daily_rollup(city=DEFAULT_CITY, **kwargs):
    """Run at midnight — compute weekly trends and weather impact."""
    import database as db

    run_id = str(uuid.uuid4())[:12]
    today  = datetime.now().date().isoformat()
    print(f"\n[Task:daily_rollup] City: {city}  date={today}")
    db.insert_pipeline_run(run_id, "city_pulse_daily", "daily_rollup", "running", city=city)

    try:
        with db.get_conn() as conn:
            week_start = (datetime.now() - timedelta(days=7)).date().isoformat()

            conn.execute("""
                INSERT INTO agg_weekly_trends
                  (city, week_start, week_end, total_trips, total_revenue,
                   avg_daily_trips, peak_day, peak_hour, computed_at)
                SELECT
                  ? as city, ? as week_start, ? as week_end,
                  SUM(total_trips), SUM(total_revenue),
                  AVG(total_trips),
                  (SELECT date FROM agg_daily_summary
                   WHERE city=? AND date >= ? ORDER BY total_trips DESC LIMIT 1),
                  (SELECT hour FROM agg_hourly_pulse
                   WHERE city=? AND date >= ? ORDER BY trip_count DESC LIMIT 1),
                  ?
                FROM agg_daily_summary WHERE city=? AND date >= ?
                ON CONFLICT(city, week_start) DO UPDATE SET
                  total_trips=excluded.total_trips,
                  total_revenue=excluded.total_revenue,
                  computed_at=excluded.computed_at
            """, (city, week_start, today,
                  city, week_start, city, week_start,
                  datetime.utcnow().isoformat(), city, week_start))

            conn.execute("""
                INSERT OR REPLACE INTO agg_weather_impact
                  (city, date, hour, trip_count, avg_fare, temp_f, condition,
                   is_rain, is_snow, wind_speed_mph, computed_at)
                SELECT
                  p.city, p.date, p.hour, p.trip_count, p.avg_fare,
                  w.temp_f, w.condition, w.is_rain, w.is_snow, w.wind_speed_mph,
                  ? as computed_at
                FROM agg_hourly_pulse p
                LEFT JOIN stg_weather w
                  ON w.city = p.city
                  AND substr(w.recorded_at,1,10) = p.date
                  AND CAST(strftime('%H', w.recorded_at) AS INTEGER) = p.hour
                WHERE p.city=? AND p.date >= ?
            """, (datetime.utcnow().isoformat(), city, week_start))

        db.update_pipeline_run(run_id, "success")
        print(f"[Task:daily_rollup] Weekly trends & weather impact updated for {city}")
        return {"run_id": run_id, "status": "success", "city": city}
    except Exception as e:
        db.update_pipeline_run(run_id, "failed", error_message=str(e))
        raise


def task_data_quality_report(city=DEFAULT_CITY, **kwargs):
    """Run DQ checks on the warehouse."""
    import database as db

    run_id = str(uuid.uuid4())[:12]
    print(f"\n[Task:dq_report] City: {city}  run_id={run_id}")
    db.insert_pipeline_run(run_id, "city_pulse_daily", "dq_report", "running", city=city)

    with db.get_conn() as conn:
        counts = db.get_data_counts(city=city)

        db.log_dq_check(run_id, "stg_trips_not_empty", "stg_taxi_trips",
                        "PASS" if counts["stg_trips"] > 0 else "FAIL",
                        ">0", counts["stg_trips"])

        hours_covered = conn.execute(
            "SELECT COUNT(DISTINCT hour) FROM agg_hourly_pulse WHERE city=?", (city,)
        ).fetchone()[0]
        db.log_dq_check(run_id, "hourly_coverage", "agg_hourly_pulse",
                        "PASS" if hours_covered >= 20 else "WARN",
                        ">=20 hours", hours_covered)

        neg_fares = conn.execute(
            "SELECT COUNT(*) FROM stg_taxi_trips WHERE city=? AND fare_amount < 0", (city,)
        ).fetchone()[0]
        db.log_dq_check(run_id, "no_negative_fares", "stg_taxi_trips",
                        "PASS" if neg_fares == 0 else "FAIL", 0, neg_fares)

        blank_hoods = conn.execute(
            "SELECT COUNT(*) FROM stg_taxi_trips WHERE city=? AND pickup_neighborhood IS NULL", (city,)
        ).fetchone()[0]
        db.log_dq_check(run_id, "no_null_neighborhoods", "stg_taxi_trips",
                        "PASS" if blank_hoods == 0 else "WARN", 0, blank_hoods)

        bad_pulse = conn.execute(
            "SELECT COUNT(*) FROM agg_hourly_pulse WHERE city=? AND (pulse_score < 0 OR pulse_score > 100)", (city,)
        ).fetchone()[0]
        db.log_dq_check(run_id, "pulse_score_range", "agg_hourly_pulse",
                        "PASS" if bad_pulse == 0 else "FAIL", "0-100", bad_pulse)

    db.update_pipeline_run(run_id, "success", metadata=counts)
    print(f"[Task:dq_report] DQ checks complete for {city}")
    return {"run_id": run_id, "city": city, "counts": counts}


# ══════════════════════════════════════════════════════════════════════════════
#  AIRFLOW DAG DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    "owner":            "city_pulse",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="city_pulse_hourly",
        description="Ingest city taxi + weather data every hour",
        default_args=DEFAULT_ARGS,
        start_date=days_ago(1),
        schedule_interval="0 * * * *",
        catchup=False,
        tags=["city_pulse", "etl"],
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
            ti          = kwargs["ti"]
            taxi_result = ti.xcom_pull(task_ids="extract_taxi")
            wx_result   = ti.xcom_pull(task_ids="extract_weather")
            return task_transform_load(taxi_result, wx_result, **kwargs)

        transform_load_task = PythonOperator(
            task_id="transform_load",
            python_callable=_transform_load_airflow,
        )
        [extract_taxi_task, extract_weather_task] >> transform_load_task

    with DAG(
        dag_id="city_pulse_daily",
        description="Daily rollup — weekly trends, DQ checks, weather impact",
        default_args=DEFAULT_ARGS,
        start_date=days_ago(1),
        schedule_interval="0 0 * * *",
        catchup=False,
        tags=["city_pulse", "analytics", "dq"],
    ) as daily_dag:

        rollup_task = PythonOperator(task_id="daily_rollup",  python_callable=task_daily_rollup)
        dq_task     = PythonOperator(task_id="dq_report",     python_callable=task_data_quality_report)
        rollup_task >> dq_task
