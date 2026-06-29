"""
scheduler.py — Built-in DAG scheduler
======================================
Runs the City Pulse DAGs on schedule without requiring Apache Airflow.
Supports any city defined in CITY_PROFILES via --city flag.

Usage:
    python scheduler.py                      # Default city (New York)
    python scheduler.py --city London        # Run for London
    python scheduler.py --backfill           # 90-day backfill
    python scheduler.py --once               # Run once and exit
    python scheduler.py --status             # Show pipeline status
"""

import sys
import os
import time
import uuid
import signal
import argparse
import threading
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import SCHEDULE_INTERVAL_MIN, TAXI_ROWS_PER_RUN, DEFAULT_CITY, SUPPORTED_CITIES
import database as db

# ── Graceful shutdown ──────────────────────────────────────────────────────────
_running = True
def _signal_handler(sig, frame):
    global _running
    print("\n[Scheduler] Shutting down gracefully …")
    _running = False

# signal.signal() only works in the main thread — crashes on Windows when
# scheduler.py is imported inside a background thread (e.g. from run.py)
import threading as _threading
if _threading.current_thread() is _threading.main_thread():
    signal.signal(signal.SIGINT,  _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)


# ── Pipeline runners ───────────────────────────────────────────────────────────
def run_hourly_pipeline(n_rows=TAXI_ROWS_PER_RUN, span_days=7, city=DEFAULT_CITY):
    from dags.city_pulse_dag import (
        task_extract_taxi, task_extract_weather, task_transform_load
    )
    run_id = str(uuid.uuid4())[:12]
    print(f"\n{'='*60}")
    print(f" [Scheduler] HOURLY RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" City: {city}  run_id={run_id}  n_rows={n_rows:,}")
    print(f"{'='*60}")

    taxi_result    = task_extract_taxi(n_rows=n_rows, span_days=span_days,
                                       run_id=run_id, dag_id="city_pulse_hourly", city=city)
    weather_result = task_extract_weather(run_id=run_id, dag_id="city_pulse_hourly", city=city)
    stats = task_transform_load(taxi_result, weather_result,
                                run_id=run_id, dag_id="city_pulse_hourly", city=city)
    return stats


def run_daily_pipeline(city=DEFAULT_CITY):
    from dags.city_pulse_dag import task_daily_rollup, task_data_quality_report
    print(f"\n[Scheduler] DAILY ROLLUP — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {city}")
    rollup_result = task_daily_rollup(dag_id="city_pulse_daily", city=city)
    dq_result     = task_data_quality_report(dag_id="city_pulse_daily", city=city)
    return {"rollup": rollup_result, "dq": dq_result}


def run_backfill(n_rows=200_000, span_days=90, city=DEFAULT_CITY):
    from data_generator import generate_batch, generate_weather_reading
    from transforms import run_etl

    print(f"\n{'='*60}")
    print(f" [Scheduler] BACKFILL — {city}")
    print(f" Generating {n_rows:,} trips over {span_days} days")
    print(f"{'='*60}")

    run_id     = "backfill_" + str(uuid.uuid4())[:8]
    chunk_size = 25_000
    all_stats  = []

    for chunk_num in range(0, n_rows, chunk_size):
        this_chunk = min(chunk_size, n_rows - chunk_num)
        print(f"[Backfill] Chunk {chunk_num//chunk_size+1}/{(n_rows//chunk_size)+1} — {this_chunk:,} trips")

        raw_trips = generate_batch(n=this_chunk, city=city, span_days=span_days)

        raw_weather = []
        for d in range(0, span_days, 5):
            dt = datetime.now() - timedelta(days=d)
            for h in [0, 6, 12, 18]:
                raw_weather.append(generate_weather_reading(city, dt.replace(hour=h)))

        stats = run_etl(raw_trips, raw_weather, city=city,
                        run_id=f"{run_id}_c{chunk_num//chunk_size}",
                        dag_id="city_pulse_backfill")
        all_stats.append(stats)

    run_daily_pipeline(city=city)

    total_loaded = sum(s["stg_trips_loaded"] for s in all_stats)
    print(f"\n[Backfill] Complete — {total_loaded:,} trips loaded for {city}")
    return {"total_loaded": total_loaded, "chunks": len(all_stats), "city": city}


def print_status(city=DEFAULT_CITY):
    counts  = db.get_data_counts(city=city)
    history = db.get_pipeline_history(10, city=city)
    kpis    = db.query_kpis(city=city)

    print(f"\n{'='*60}")
    print(f"  City Pulse ETL — Status for {city}")
    print(f"{'='*60}")
    print(f"\n  DATA WAREHOUSE COUNTS:")
    for k, v in counts.items():
        print(f"    {k:<20}: {v:>10,}")
    print(f"\n  Total trips   : {kpis['total_trips']:,}")
    print(f"  Total revenue : {kpis['total_revenue']:,.0f}")
    print(f"  Avg fare      : {kpis['avg_fare']:.2f}")
    print(f"  Busiest hour  : {kpis['busiest_hour'] or '—'}")
    print(f"  Last run      : {kpis['last_run'] or '—'}")
    print(f"\n{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="City Pulse ETL Scheduler")
    parser.add_argument("--city",     type=str, default=DEFAULT_CITY,
                        choices=SUPPORTED_CITIES,
                        help=f"City to run pipeline for (default: {DEFAULT_CITY})")
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--once",     action="store_true")
    parser.add_argument("--status",   action="store_true")
    parser.add_argument("--rows",     type=int, default=TAXI_ROWS_PER_RUN)
    parser.add_argument("--backfill-rows", type=int, default=200_000)
    args = parser.parse_args()

    db.init_db()

    if args.status:
        print_status(city=args.city)
        return

    print(f"\n  City Pulse Scheduler — City: {args.city}\n  Dashboard: http://localhost:5050\n")

    if args.backfill:
        run_backfill(n_rows=args.backfill_rows, span_days=90, city=args.city)

    if args.once:
        run_hourly_pipeline(n_rows=args.rows, city=args.city)
        run_daily_pipeline(city=args.city)
        print_status(city=args.city)
        return

    last_hourly = None
    last_daily  = None

    try:
        run_hourly_pipeline(n_rows=args.rows, city=args.city)
        last_hourly = datetime.now()
        run_daily_pipeline(city=args.city)
        last_daily = datetime.now()
        print_status(city=args.city)
    except Exception as e:
        print(f"[Scheduler] Initial run error: {e}")

    while _running:
        now = datetime.now()
        if last_hourly is None or (now - last_hourly).total_seconds() >= SCHEDULE_INTERVAL_MIN * 60:
            try:
                run_hourly_pipeline(n_rows=args.rows, city=args.city)
                last_hourly = now
            except Exception as e:
                print(f"[Scheduler] Hourly run error: {e}")

        if last_daily is None or (now - last_daily).total_seconds() >= 86400:
            try:
                run_daily_pipeline(city=args.city)
                last_daily = now
            except Exception as e:
                print(f"[Scheduler] Daily rollup error: {e}")

        elapsed = (datetime.now() - last_hourly).total_seconds() if last_hourly else 0
        wait    = max(0, SCHEDULE_INTERVAL_MIN * 60 - elapsed)
        print(f"\n[Scheduler] Next run in {wait/60:.1f} minutes. Press Ctrl+C to stop.")
        for _ in range(int(wait)):
            if not _running:
                break
            time.sleep(1)

    print("[Scheduler] Stopped.")


if __name__ == "__main__":
    main()
