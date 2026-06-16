"""
database.py — SQLite data warehouse for City Pulse
Schema: raw → staging → analytics (star schema)
"""

import sqlite3
import json
import os
from datetime import datetime
from config import DB_PATH


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables. Safe to call multiple times."""
    with get_conn() as conn:
        conn.executescript("""
        -- ═══════════════════════════════════════════
        -- RAW LAYER — exactly as received
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS raw_taxi_trips (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at     TEXT NOT NULL,
            source          TEXT NOT NULL DEFAULT 'nyc_tlc',
            raw_json        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS raw_weather (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at     TEXT NOT NULL,
            city            TEXT NOT NULL,
            raw_json        TEXT NOT NULL
        );

        -- ═══════════════════════════════════════════
        -- STAGING LAYER — cleaned, typed, validated
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS stg_taxi_trips (
            trip_id             TEXT PRIMARY KEY,
            pickup_datetime     TEXT NOT NULL,
            dropoff_datetime    TEXT NOT NULL,
            pickup_hour         INTEGER NOT NULL,
            pickup_weekday      INTEGER NOT NULL,   -- 0=Mon, 6=Sun
            pickup_weekday_name TEXT NOT NULL,
            pickup_date         TEXT NOT NULL,
            pickup_neighborhood TEXT,
            dropoff_neighborhood TEXT,
            passenger_count     INTEGER,
            trip_distance_miles REAL,
            fare_amount         REAL,
            tip_amount          REAL,
            total_amount        REAL,
            payment_type        TEXT,
            trip_duration_min   REAL,
            speed_mph           REAL,
            is_surge_hour       INTEGER DEFAULT 0,  -- 1 if 7-10am or 4-8pm
            is_weekend          INTEGER DEFAULT 0,
            is_airport_trip     INTEGER DEFAULT 0,
            loaded_at           TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stg_weather (
            weather_id      TEXT PRIMARY KEY,
            recorded_at     TEXT NOT NULL,
            city            TEXT NOT NULL,
            temp_f          REAL,
            feels_like_f    REAL,
            humidity_pct    INTEGER,
            wind_speed_mph  REAL,
            condition       TEXT,
            condition_code  INTEGER,
            visibility_mi   REAL,
            is_rain         INTEGER DEFAULT 0,
            is_snow         INTEGER DEFAULT 0,
            loaded_at       TEXT NOT NULL
        );

        -- ═══════════════════════════════════════════
        -- ANALYTICS LAYER — aggregated facts
        -- ═══════════════════════════════════════════

        -- Hourly "pulse" — the main city alive metric
        CREATE TABLE IF NOT EXISTS agg_hourly_pulse (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            hour            INTEGER NOT NULL,
            day_of_week     INTEGER NOT NULL,
            day_name        TEXT NOT NULL,
            trip_count      INTEGER DEFAULT 0,
            total_passengers INTEGER DEFAULT 0,
            avg_fare        REAL,
            avg_distance    REAL,
            avg_duration_min REAL,
            avg_speed_mph   REAL,
            total_revenue   REAL,
            tip_rate_pct    REAL,
            surge_trip_pct  REAL,
            airport_pct     REAL,
            pulse_score     REAL,   -- composite 0–100 "alive" score
            computed_at     TEXT NOT NULL,
            UNIQUE(date, hour)
        );

        -- Neighborhood activity
        CREATE TABLE IF NOT EXISTS agg_neighborhood_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            neighborhood    TEXT NOT NULL,
            trip_count      INTEGER DEFAULT 0,
            avg_fare        REAL,
            total_revenue   REAL,
            avg_distance    REAL,
            rank_by_trips   INTEGER,
            computed_at     TEXT NOT NULL,
            UNIQUE(date, neighborhood)
        );

        -- Daily summary
        CREATE TABLE IF NOT EXISTS agg_daily_summary (
            date            TEXT PRIMARY KEY,
            total_trips     INTEGER DEFAULT 0,
            total_passengers INTEGER DEFAULT 0,
            total_revenue   REAL,
            avg_fare        REAL,
            avg_distance    REAL,
            avg_duration_min REAL,
            busiest_hour    INTEGER,
            quietest_hour   INTEGER,
            peak_neighborhood TEXT,
            weekend_flag    INTEGER DEFAULT 0,
            computed_at     TEXT NOT NULL
        );

        -- Weekly trends
        CREATE TABLE IF NOT EXISTS agg_weekly_trends (
            week_start      TEXT PRIMARY KEY,
            week_end        TEXT,
            total_trips     INTEGER DEFAULT 0,
            total_revenue   REAL,
            avg_daily_trips REAL,
            peak_day        TEXT,
            peak_hour       INTEGER,
            computed_at     TEXT NOT NULL
        );

        -- Weather impact
        CREATE TABLE IF NOT EXISTS agg_weather_impact (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT NOT NULL,
            hour            INTEGER NOT NULL,
            trip_count      INTEGER,
            avg_fare        REAL,
            temp_f          REAL,
            condition       TEXT,
            is_rain         INTEGER,
            is_snow         INTEGER,
            wind_speed_mph  REAL,
            computed_at     TEXT NOT NULL,
            UNIQUE(date, hour)
        );

        -- ═══════════════════════════════════════════
        -- PIPELINE METADATA
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          TEXT PRIMARY KEY,
            dag_id          TEXT NOT NULL,
            task_id         TEXT NOT NULL,
            status          TEXT NOT NULL,   -- running/success/failed/skipped
            started_at      TEXT NOT NULL,
            finished_at     TEXT,
            rows_extracted  INTEGER DEFAULT 0,
            rows_loaded     INTEGER DEFAULT 0,
            rows_rejected   INTEGER DEFAULT 0,
            error_message   TEXT,
            metadata_json   TEXT
        );

        CREATE TABLE IF NOT EXISTS data_quality_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          TEXT,
            check_name      TEXT NOT NULL,
            table_name      TEXT NOT NULL,
            status          TEXT NOT NULL,   -- PASS/FAIL/WARN
            expected        TEXT,
            actual          TEXT,
            logged_at       TEXT NOT NULL
        );

        -- ═══════════════════════════════════════════
        -- INDEXES
        -- ═══════════════════════════════════════════
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_hour     ON stg_taxi_trips(pickup_hour);
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_date     ON stg_taxi_trips(pickup_date);
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_hood     ON stg_taxi_trips(pickup_neighborhood);
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_weekday  ON stg_taxi_trips(pickup_weekday);
        CREATE INDEX IF NOT EXISTS idx_agg_pulse_date    ON agg_hourly_pulse(date);
        CREATE INDEX IF NOT EXISTS idx_pipeline_dag      ON pipeline_runs(dag_id);
        CREATE INDEX IF NOT EXISTS idx_pipeline_status   ON pipeline_runs(status);
        """)
    print("[DB] Schema initialized ✓")


# ── Query helpers ──────────────────────────────────────────────────────────────
def insert_pipeline_run(run_id, dag_id, task_id, status="running"):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO pipeline_runs
            (run_id, dag_id, task_id, status, started_at)
            VALUES (?,?,?,?,?)
        """, (run_id, dag_id, task_id, status, now))


def update_pipeline_run(run_id, status, rows_extracted=0,
                         rows_loaded=0, rows_rejected=0,
                         error_message=None, metadata=None):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("""
            UPDATE pipeline_runs SET
                status=?, finished_at=?, rows_extracted=?,
                rows_loaded=?, rows_rejected=?, error_message=?,
                metadata_json=?
            WHERE run_id=?
        """, (status, now, rows_extracted, rows_loaded, rows_rejected,
              error_message, json.dumps(metadata) if metadata else None, run_id))


def log_dq_check(run_id, check_name, table_name, status, expected, actual):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO data_quality_log
            (run_id, check_name, table_name, status, expected, actual, logged_at)
            VALUES (?,?,?,?,?,?,?)
        """, (run_id, check_name, table_name, status,
              str(expected), str(actual), now))


def get_pipeline_history(limit=50):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM pipeline_runs
            ORDER BY started_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_dq_summary(run_id=None):
    with get_conn() as conn:
        if run_id:
            rows = conn.execute(
                "SELECT * FROM data_quality_log WHERE run_id=? ORDER BY logged_at DESC",
                (run_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM data_quality_log ORDER BY logged_at DESC LIMIT 100"
            ).fetchall()
        return [dict(r) for r in rows]


# ── Analytics queries ──────────────────────────────────────────────────────────
def query_hourly_pulse(date=None, days=7):
    with get_conn() as conn:
        if date:
            rows = conn.execute("""
                SELECT * FROM agg_hourly_pulse
                WHERE date=? ORDER BY hour
            """, (date,)).fetchall()
        else:
            rows = conn.execute(f"""
                SELECT * FROM agg_hourly_pulse
                WHERE date >= date('now','-{days} days')
                ORDER BY date, hour
            """).fetchall()
        return [dict(r) for r in rows]


def query_neighborhood_stats(date=None):
    with get_conn() as conn:
        if date:
            rows = conn.execute("""
                SELECT * FROM agg_neighborhood_stats
                WHERE date=? ORDER BY trip_count DESC
            """, (date,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT neighborhood,
                       SUM(trip_count) as total_trips,
                       AVG(avg_fare)   as avg_fare,
                       SUM(total_revenue) as total_revenue,
                       AVG(avg_distance) as avg_distance
                FROM agg_neighborhood_stats
                GROUP BY neighborhood
                ORDER BY total_trips DESC
            """).fetchall()
        return [dict(r) for r in rows]


def query_daily_summary(days=30):
    with get_conn() as conn:
        rows = conn.execute(f"""
            SELECT * FROM agg_daily_summary
            WHERE date >= date('now','-{days} days')
            ORDER BY date DESC
        """).fetchall()
        return [dict(r) for r in rows]


def query_weekday_pattern():
    """Average trips by hour and weekday — the 'city pulse' grid."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT day_of_week, day_name, hour,
                   AVG(trip_count) as avg_trips,
                   AVG(pulse_score) as avg_pulse,
                   AVG(avg_fare)   as avg_fare
            FROM agg_hourly_pulse
            GROUP BY day_of_week, hour
            ORDER BY day_of_week, hour
        """).fetchall()
        return [dict(r) for r in rows]


def query_weather_impact():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM agg_weather_impact
            ORDER BY date DESC, hour DESC LIMIT 200
        """).fetchall()
        return [dict(r) for r in rows]


def query_top_hours(top_n=5):
    """Which hours are Manhattan most alive?"""
    with get_conn() as conn:
        rows = conn.execute(f"""
            SELECT hour,
                   AVG(trip_count) as avg_trips,
                   AVG(pulse_score) as avg_pulse,
                   AVG(avg_fare)   as avg_fare,
                   AVG(total_revenue) as avg_revenue,
                   COUNT(*) as data_points
            FROM agg_hourly_pulse
            GROUP BY hour
            ORDER BY avg_pulse DESC
            LIMIT ?
        """, (top_n,)).fetchall()
        return [dict(r) for r in rows]


def query_kpis():
    """High-level numbers for the dashboard header."""
    with get_conn() as conn:
        r = {}
        r["total_trips"]   = conn.execute("SELECT COUNT(*) FROM stg_taxi_trips").fetchone()[0]
        r["total_revenue"] = conn.execute("SELECT COALESCE(SUM(total_amount),0) FROM stg_taxi_trips").fetchone()[0]
        r["avg_fare"]      = conn.execute("SELECT COALESCE(AVG(fare_amount),0) FROM stg_taxi_trips").fetchone()[0]
        r["avg_speed"]     = conn.execute("SELECT COALESCE(AVG(speed_mph),0) FROM stg_taxi_trips WHERE speed_mph > 0").fetchone()[0]
        r["pipeline_runs"] = conn.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status='success'").fetchone()[0]
        r["last_run"]      = conn.execute(
            "SELECT finished_at FROM pipeline_runs WHERE status='success' ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()
        r["last_run"] = r["last_run"][0] if r["last_run"] else None

        # Busiest hour ever
        bh = conn.execute("""
            SELECT hour, MAX(avg_trips) FROM (
                SELECT hour, AVG(trip_count) as avg_trips
                FROM agg_hourly_pulse GROUP BY hour
            )
        """).fetchone()
        r["busiest_hour"] = bh[0] if bh else None

        return r


def get_recent_trips(limit=100):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM stg_taxi_trips ORDER BY pickup_datetime DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_data_counts():
    with get_conn() as conn:
        return {
            "raw_trips":    conn.execute("SELECT COUNT(*) FROM raw_taxi_trips").fetchone()[0],
            "stg_trips":    conn.execute("SELECT COUNT(*) FROM stg_taxi_trips").fetchone()[0],
            "agg_hourly":   conn.execute("SELECT COUNT(*) FROM agg_hourly_pulse").fetchone()[0],
            "agg_daily":    conn.execute("SELECT COUNT(*) FROM agg_daily_summary").fetchone()[0],
            "agg_nbhd":     conn.execute("SELECT COUNT(*) FROM agg_neighborhood_stats").fetchone()[0],
            "weather_rows": conn.execute("SELECT COUNT(*) FROM stg_weather").fetchone()[0],
            "dq_checks":    conn.execute("SELECT COUNT(*) FROM data_quality_log").fetchone()[0],
            "pipeline_runs":conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0],
        }
