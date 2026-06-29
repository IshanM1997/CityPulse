"""
run.py — City Pulse — One-command launcher
==========================================
Starts both the ETL scheduler and dashboard server in parallel threads.

Usage:
    python run.py                    # Start everything
    python run.py --backfill         # Seed 90 days of history first, then start
    python run.py --dashboard-only   # Dashboard only (no scheduler)
    python run.py --etl-only         # ETL only (no dashboard)
    python run.py --status           # Show pipeline status
"""

import sys
import os
import time
import argparse
import threading
import signal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import database as db
from config import DASHBOARD_PORT

_shutdown = False

def _sig(sig, frame):
    global _shutdown
    print("\n[Run] Shutting down…")
    _shutdown = True
    sys.exit(0)

signal.signal(signal.SIGINT,  _sig)
signal.signal(signal.SIGTERM, _sig)


def start_dashboard():
    from dashboard import app
    from config import DASHBOARD_HOST, DASHBOARD_PORT
    print(f"[Dashboard] Starting on http://localhost:{DASHBOARD_PORT}")
    app.run(host=DASHBOARD_HOST, port=DASHBOARD_PORT, debug=False,
            threaded=True, use_reloader=False)


def start_scheduler(backfill=False, n_rows=50_000):
    from scheduler import run_backfill, run_hourly_pipeline, run_daily_pipeline
    from config import SCHEDULE_INTERVAL_MIN
    import time

    if backfill:
        print("[Scheduler] Running 90-day backfill …")
        run_backfill(n_rows=n_rows, span_days=90)
    else:
        print("[Scheduler] Running initial ETL pass …")
        try:
            run_hourly_pipeline(n_rows=n_rows, span_days=30)
            run_daily_pipeline()
        except Exception as e:
            print(f"[Scheduler] Initial run error: {e}")

    # Loop
    while not _shutdown:
        for _ in range(SCHEDULE_INTERVAL_MIN * 60):
            if _shutdown: return
            time.sleep(1)
        try:
            run_hourly_pipeline(n_rows=n_rows, span_days=7)
        except Exception as e:
            print(f"[Scheduler] Loop error: {e}")


def main():
    parser = argparse.ArgumentParser(description="City Pulse ETL Pipeline")
    parser.add_argument("--backfill",      action="store_true",
                        help="Seed 90 days of history before starting")
    parser.add_argument("--dashboard-only",action="store_true",
                        help="Start dashboard only, no ETL scheduler")
    parser.add_argument("--etl-only",      action="store_true",
                        help="Run ETL only, no dashboard")
    parser.add_argument("--status",        action="store_true",
                        help="Show pipeline status and exit")
    parser.add_argument("--rows",          type=int, default=50_000,
                        help="Rows per ETL run (default: 50,000)")
    args = parser.parse_args()

    db.init_db()

    if args.status:
        from scheduler import print_status
        print_status()
        return

    print(f"""
╔══════════════════════════════════════════════════════════╗
║   🏙  City Pulse — NYC ETL Pipeline                    ║
╚══════════════════════════════════════════════════════════╝
  Dashboard : http://localhost:{DASHBOARD_PORT}
  Schedule  : every 60 minutes
  Rows/run  : {args.rows:,}

  Tips:
    - Open the dashboard in your browser
    - Click "Run ETL (10k)" for a quick test
    - Click "Backfill 90 Days" for full history
    - Or run: python run.py --backfill --rows 200000

  Press Ctrl+C to stop all processes.
""")

    threads = []

    if not args.etl_only:
        dash_thread = threading.Thread(target=start_dashboard, daemon=True, name="Dashboard")
        dash_thread.start()
        threads.append(dash_thread)
        time.sleep(1.5)  # Give Flask a moment to bind the port

    if not args.dashboard_only:
        sched_thread = threading.Thread(
            target=start_scheduler,
            kwargs={"backfill": args.backfill, "n_rows": args.rows},
            daemon=True, name="Scheduler"
        )
        sched_thread.start()
        threads.append(sched_thread)

    # Keep main thread alive
    try:
        while not _shutdown:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass

    print("[Run] All processes stopped.")


if __name__ == "__main__":
    main()
    
