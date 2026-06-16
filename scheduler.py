"""
scheduler.py — Built-in DAG scheduler
======================================
Runs the City Pulse DAGs on schedule without requiring Apache Airflow.
Airflow-compatible: same task functions, same pipeline logic.

Usage:
    python scheduler.py              # Start scheduler (runs every hour)
    python scheduler.py --backfill   # Generate 90 days of history first
    python scheduler.py --once       # Run once and exit
    python scheduler.py --status     # Show pipeline status
"""

import sys
import os
import time
import uuid
import json
import signal
import argparse
import threading
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import SCHEDULE_INTERVAL_MIN, TAXI_ROWS_PER_RUN
import database as db

# ── Graceful shutdown ──────────────────────────────────────────────────────────
_running = True
def _signal_handler(sig, frame):
    global _running
    print("\n[Scheduler] Shutting down gracefully …")
    _running = False

signal.signal(signal.SIGINT,  _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ── Pipeline runner ────────────────────────────────────────────────────────────
def run_hourly_pipeline(n_rows: int = TAXI_ROWS_PER_RUN,
                        span_days: int = 7) -> dict:
    """Execute the hourly ETL pipeline."""
    from dags.city_pulse_dag import (
        task_extract_taxi, task_extract_weather, task_transform_load
    )

    run_id = str(uuid.uuid4())[:12]
    print(f"\n{'='*60}")
    print(f" [Scheduler] HOURLY RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" run_id={run_id}  n_rows={n_rows:,}")
    print(f"{'='*60}")

    # Task 1 & 2: Extract (parallel in real Airflow; sequential here)
    taxi_result    = task_extract_taxi(n_rows=n_rows, span_days=span_days,
                                       run_id=run_id, dag_id="city_pulse_hourly")
    weather_result = task_extract_weather(run_id=run_id, dag_id="city_pulse_hourly")

    # Task 3: Transform + Load
    stats = task_transform_load(taxi_result, weather_result,
                                run_id=run_id, dag_id="city_pulse_hourly")
    return stats


def run_daily_pipeline() -> dict:
    """Execute the daily rollup pipeline."""
    from dags.city_pulse_dag import task_daily_rollup, task_data_quality_report

    print(f"\n[Scheduler] DAILY ROLLUP — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    rollup_result = task_daily_rollup(dag_id="city_pulse_daily")
    dq_result     = task_data_quality_report(dag_id="city_pulse_daily")
    return {"rollup": rollup_result, "dq": dq_result}


def run_backfill(n_rows: int = 200_000, span_days: int = 90) -> dict:
    """Generate 90 days of historical data."""
    from data_generator import generate_batch, generate_weather_reading
    from transforms import run_etl

    print(f"\n{'='*60}")
    print(f" [Scheduler] BACKFILL — generating {n_rows:,} trips over {span_days} days")
    print(f"{'='*60}")

    run_id = "backfill_" + str(uuid.uuid4())[:8]

    # Generate in chunks so progress is visible
    chunk_size = 25_000
    all_stats  = []

    print(f"[Backfill] Generating {n_rows:,} trips in chunks of {chunk_size:,} …")
    for chunk_num in range(0, n_rows, chunk_size):
        this_chunk = min(chunk_size, n_rows - chunk_num)
        print(f"[Backfill] Chunk {chunk_num//chunk_size+1}/{(n_rows//chunk_size)+1} — {this_chunk:,} trips")

        raw_trips = generate_batch(n=this_chunk, span_days=span_days)

        # Generate matching weather
        raw_weather = []
        for d in range(0, span_days, 5):   # every 5 days
            from datetime import datetime, timedelta
            dt = datetime.now() - timedelta(days=d)
            for h in [0, 6, 12, 18]:
                raw_weather.append(generate_weather_reading(
                    "New York", dt.replace(hour=h)
                ))

        stats = run_etl(raw_trips, raw_weather,
                        run_id=f"{run_id}_c{chunk_num//chunk_size}",
                        dag_id="city_pulse_backfill")
        all_stats.append(stats)

    # Run daily rollup once after backfill
    run_daily_pipeline()

    total_loaded = sum(s["stg_trips_loaded"] for s in all_stats)
    print(f"\n[Backfill] ✅ Complete — {total_loaded:,} trips loaded into warehouse")
    return {"total_loaded": total_loaded, "chunks": len(all_stats)}


# ── Status display ─────────────────────────────────────────────────────────────
def print_status():
    """Print current pipeline status."""
    counts  = db.get_data_counts()
    history = db.get_pipeline_history(10)
    kpis    = db.query_kpis()

    print(f"\n{'═'*60}")
    print(f"  🏙  City Pulse ETL — Pipeline Status")
    print(f"{'═'*60}")
    print(f"\n  DATA WAREHOUSE COUNTS:")
    for k, v in counts.items():
        print(f"    {k:<20}: {v:>10,}")

    print(f"\n  KEY METRICS:")
    print(f"    Total trips analyzed : {kpis['total_trips']:>10,}")
    print(f"    Total revenue        : ${kpis['total_revenue']:>10,.0f}")
    print(f"    Avg fare             : ${kpis['avg_fare']:>10.2f}")
    print(f"    Avg speed (mph)      : {kpis['avg_speed']:>10.1f}")
    print(f"    Busiest hour         : {kpis['busiest_hour'] or '—':>10}")
    print(f"    Pipeline runs        : {kpis['pipeline_runs']:>10,}")
    print(f"    Last successful run  : {kpis['last_run'] or '—'}")

    print(f"\n  RECENT PIPELINE RUNS:")
    print(f"  {'Run ID':<15} {'DAG':<22} {'Status':<10} {'Loaded':>8} {'Started':<22}")
    print(f"  {'-'*80}")
    for r in history[:10]:
        status_icon = {"success":"✅","failed":"❌","running":"🔄","skipped":"⏭"}.get(r["status"],"❓")
        print(f"  {r['run_id']:<15} {r['dag_id']:<22} "
              f"{status_icon} {r['status']:<8} {r['rows_loaded']:>8,} "
              f"  {r['started_at'][:19]}")

    top = db.query_top_hours(5)
    if top:
        print(f"\n  ⚡ WHICH HOUR IS MANHATTAN MOST ALIVE?")
        print(f"  {'Hour':<8} {'Avg Trips':>10} {'Pulse Score':>12} {'Avg Fare':>10}")
        print(f"  {'-'*45}")
        for h in top:
            hour_str = f"{h['hour']:02d}:00"
            print(f"  {hour_str:<8} {h['avg_trips']:>10.0f} "
                  f"{h['avg_pulse']:>12.1f} "
                  f"${h['avg_fare']:>9.2f}")

    print(f"\n{'═'*60}\n")


# ── Main scheduler loop ────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="City Pulse ETL Scheduler")
    parser.add_argument("--backfill",  action="store_true", help="Run 90-day backfill first")
    parser.add_argument("--once",      action="store_true", help="Run pipeline once and exit")
    parser.add_argument("--status",    action="store_true", help="Show status and exit")
    parser.add_argument("--rows",      type=int, default=TAXI_ROWS_PER_RUN,
                        help=f"Rows per run (default: {TAXI_ROWS_PER_RUN:,})")
    parser.add_argument("--backfill-rows", type=int, default=200_000,
                        help="Rows for backfill (default: 200,000)")
    args = parser.parse_args()

    # Init DB
    db.init_db()

    if args.status:
        print_status()
        return

    print(f"""
╔══════════════════════════════════════════════════════╗
║   🏙  City Pulse — NYC ETL Pipeline Scheduler        ║
╚══════════════════════════════════════════════════════╝
  Schedule : every {SCHEDULE_INTERVAL_MIN} minutes
  Rows/run : {args.rows:,}
  DB       : {db.DB_PATH}
  Dashboard: http://localhost:5050
  Press Ctrl+C to stop
""")

    # Backfill if requested
    if args.backfill:
        print("[Scheduler] Running backfill first …")
        run_backfill(n_rows=args.backfill_rows, span_days=90)

    # Single run mode
    if args.once:
        run_hourly_pipeline(n_rows=args.rows)
        run_daily_pipeline()
        print_status()
        return

    # Continuous scheduler
    last_hourly = None
    last_daily  = None

    # Run immediately on startup
    print("[Scheduler] Starting initial run …")
    try:
        run_hourly_pipeline(n_rows=args.rows)
        last_hourly = datetime.now()
        run_daily_pipeline()
        last_daily = datetime.now()
        print_status()
    except Exception as e:
        print(f"[Scheduler] Initial run error: {e}")

    while _running:
        now = datetime.now()

        # Hourly check
        if last_hourly is None or (now - last_hourly).total_seconds() >= SCHEDULE_INTERVAL_MIN * 60:
            try:
                run_hourly_pipeline(n_rows=args.rows)
                last_hourly = now
            except Exception as e:
                print(f"[Scheduler] Hourly run error: {e}")

        # Daily check (midnight-ish)
        if last_daily is None or (now - last_daily).total_seconds() >= 86400:
            try:
                run_daily_pipeline()
                last_daily = now
            except Exception as e:
                print(f"[Scheduler] Daily rollup error: {e}")

        # Sleep in small increments so Ctrl+C works
        next_run = SCHEDULE_INTERVAL_MIN * 60
        elapsed  = (datetime.now() - last_hourly).total_seconds() if last_hourly else 0
        wait     = max(0, next_run - elapsed)
        print(f"\n[Scheduler] Next run in {wait/60:.1f} minutes. Press Ctrl+C to stop.")

        for _ in range(int(wait)):
            if not _running:
                break
            time.sleep(1)

    print("[Scheduler] Stopped.")


if __name__ == "__main__":
    main()
