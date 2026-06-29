"""
database.py — SQLite data warehouse for City Pulse
Schema: raw → staging → analytics (star schema)
All tables include a `city` column for multi-city support.
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
        -- RAW LAYER
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS raw_taxi_trips (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at TEXT NOT NULL,
            city        TEXT NOT NULL DEFAULT 'New York',
            source      TEXT NOT NULL DEFAULT 'synthetic',
            raw_json    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS raw_weather (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at TEXT NOT NULL,
            city        TEXT NOT NULL,
            raw_json    TEXT NOT NULL
        );

        -- ═══════════════════════════════════════════
        -- STAGING LAYER
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS stg_taxi_trips (
            trip_id              TEXT PRIMARY KEY,
            city                 TEXT NOT NULL DEFAULT 'New York',
            pickup_datetime      TEXT NOT NULL,
            dropoff_datetime     TEXT NOT NULL,
            pickup_hour          INTEGER NOT NULL,
            pickup_weekday       INTEGER NOT NULL,
            pickup_weekday_name  TEXT NOT NULL,
            pickup_date          TEXT NOT NULL,
            pickup_neighborhood  TEXT,
            dropoff_neighborhood TEXT,
            passenger_count      INTEGER,
            trip_distance_miles  REAL,
            fare_amount          REAL,
            tip_amount           REAL,
            total_amount         REAL,
            payment_type         TEXT,
            trip_duration_min    REAL,
            speed_mph            REAL,
            is_surge_hour        INTEGER DEFAULT 0,
            is_weekend           INTEGER DEFAULT 0,
            is_airport_trip      INTEGER DEFAULT 0,
            loaded_at            TEXT NOT NULL
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
        -- ANALYTICS LAYER
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS agg_hourly_pulse (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            city             TEXT NOT NULL DEFAULT 'New York',
            date             TEXT NOT NULL,
            hour             INTEGER NOT NULL,
            day_of_week      INTEGER NOT NULL,
            day_name         TEXT NOT NULL,
            trip_count       INTEGER DEFAULT 0,
            total_passengers INTEGER DEFAULT 0,
            avg_fare         REAL,
            avg_distance     REAL,
            avg_duration_min REAL,
            avg_speed_mph    REAL,
            total_revenue    REAL,
            tip_rate_pct     REAL,
            surge_trip_pct   REAL,
            airport_pct      REAL,
            pulse_score      REAL,
            computed_at      TEXT NOT NULL,
            UNIQUE(city, date, hour)
        );

        CREATE TABLE IF NOT EXISTS agg_neighborhood_stats (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            city          TEXT NOT NULL DEFAULT 'New York',
            date          TEXT NOT NULL,
            neighborhood  TEXT NOT NULL,
            trip_count    INTEGER DEFAULT 0,
            avg_fare      REAL,
            total_revenue REAL,
            avg_distance  REAL,
            rank_by_trips INTEGER,
            computed_at   TEXT NOT NULL,
            UNIQUE(city, date, neighborhood)
        );

        CREATE TABLE IF NOT EXISTS agg_daily_summary (
            city             TEXT NOT NULL DEFAULT 'New York',
            date             TEXT NOT NULL,
            total_trips      INTEGER DEFAULT 0,
            total_passengers INTEGER DEFAULT 0,
            total_revenue    REAL,
            avg_fare         REAL,
            avg_distance     REAL,
            avg_duration_min REAL,
            busiest_hour     INTEGER,
            quietest_hour    INTEGER,
            peak_neighborhood TEXT,
            weekend_flag     INTEGER DEFAULT 0,
            computed_at      TEXT NOT NULL,
            PRIMARY KEY(city, date)
        );

        CREATE TABLE IF NOT EXISTS agg_weekly_trends (
            city            TEXT NOT NULL DEFAULT 'New York',
            week_start      TEXT NOT NULL,
            week_end        TEXT,
            total_trips     INTEGER DEFAULT 0,
            total_revenue   REAL,
            avg_daily_trips REAL,
            peak_day        TEXT,
            peak_hour       INTEGER,
            computed_at     TEXT NOT NULL,
            PRIMARY KEY(city, week_start)
        );

        CREATE TABLE IF NOT EXISTS agg_weather_impact (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            city           TEXT NOT NULL DEFAULT 'New York',
            date           TEXT NOT NULL,
            hour           INTEGER NOT NULL,
            trip_count     INTEGER,
            avg_fare       REAL,
            temp_f         REAL,
            condition      TEXT,
            is_rain        INTEGER,
            is_snow        INTEGER,
            wind_speed_mph REAL,
            computed_at    TEXT NOT NULL,
            UNIQUE(city, date, hour)
        );

        -- ═══════════════════════════════════════════
        -- PIPELINE METADATA
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id         TEXT PRIMARY KEY,
            dag_id         TEXT NOT NULL,
            task_id        TEXT NOT NULL,
            city           TEXT NOT NULL DEFAULT 'New York',
            status         TEXT NOT NULL,
            started_at     TEXT NOT NULL,
            finished_at    TEXT,
            rows_extracted INTEGER DEFAULT 0,
            rows_loaded    INTEGER DEFAULT 0,
            rows_rejected  INTEGER DEFAULT 0,
            error_message  TEXT,
            metadata_json  TEXT
        );

        CREATE TABLE IF NOT EXISTS data_quality_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id     TEXT,
            check_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            status     TEXT NOT NULL,
            expected   TEXT,
            actual     TEXT,
            logged_at  TEXT NOT NULL
        );

        -- ═══════════════════════════════════════════
        -- INDEXES
        -- ═══════════════════════════════════════════
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_city    ON stg_taxi_trips(city);
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_hour    ON stg_taxi_trips(pickup_hour);
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_date    ON stg_taxi_trips(pickup_date);
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_hood    ON stg_taxi_trips(pickup_neighborhood);
        CREATE INDEX IF NOT EXISTS idx_stg_taxi_weekday ON stg_taxi_trips(pickup_weekday);
        CREATE INDEX IF NOT EXISTS idx_agg_pulse_city   ON agg_hourly_pulse(city);
        CREATE INDEX IF NOT EXISTS idx_agg_pulse_date   ON agg_hourly_pulse(date);
        CREATE INDEX IF NOT EXISTS idx_pipeline_dag     ON pipeline_runs(dag_id);
        CREATE INDEX IF NOT EXISTS idx_pipeline_city    ON pipeline_runs(city);
        CREATE INDEX IF NOT EXISTS idx_pipeline_status  ON pipeline_runs(status);
        """)
    print("[DB] Schema initialized ✓")


# ── Pipeline metadata helpers ──────────────────────────────────────────────────
def insert_pipeline_run(run_id, dag_id, task_id, status="running", city="New York"):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO pipeline_runs
            (run_id, dag_id, task_id, city, status, started_at)
            VALUES (?,?,?,?,?,?)
        """, (run_id, dag_id, task_id, city, status, now))


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


def get_pipeline_history(limit=50, city=None):
    with get_conn() as conn:
        if city:
            rows = conn.execute("""
                SELECT * FROM pipeline_runs WHERE city=?
                ORDER BY started_at DESC LIMIT ?
            """, (city, limit)).fetchall()
        else:
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


# ── Analytics queries (all city-filtered) ─────────────────────────────────────
def query_hourly_pulse(city="New York", days=7):
    with get_conn() as conn:
        rows = conn.execute(f"""
            SELECT * FROM agg_hourly_pulse
            WHERE city=? AND date >= date('now','-{days} days')
            ORDER BY date, hour
        """, (city,)).fetchall()
        return [dict(r) for r in rows]


def query_neighborhood_stats(city="New York"):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT neighborhood,
                   SUM(trip_count)    as total_trips,
                   AVG(avg_fare)      as avg_fare,
                   SUM(total_revenue) as total_revenue,
                   AVG(avg_distance)  as avg_distance
            FROM agg_neighborhood_stats
            WHERE city=?
            GROUP BY neighborhood
            ORDER BY total_trips DESC
        """, (city,)).fetchall()
        return [dict(r) for r in rows]


def query_daily_summary(city="New York", days=30):
    with get_conn() as conn:
        rows = conn.execute(f"""
            SELECT * FROM agg_daily_summary
            WHERE city=? AND date >= date('now','-{days} days')
            ORDER BY date DESC
        """, (city,)).fetchall()
        return [dict(r) for r in rows]


def query_weekday_pattern(city="New York"):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT day_of_week, day_name, hour,
                   AVG(trip_count)  as avg_trips,
                   AVG(pulse_score) as avg_pulse,
                   AVG(avg_fare)    as avg_fare
            FROM agg_hourly_pulse
            WHERE city=?
            GROUP BY day_of_week, hour
            ORDER BY day_of_week, hour
        """, (city,)).fetchall()
        return [dict(r) for r in rows]


def query_weather_impact(city="New York"):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM agg_weather_impact
            WHERE city=?
            ORDER BY date DESC, hour DESC LIMIT 200
        """, (city,)).fetchall()
        return [dict(r) for r in rows]


def query_top_hours(city="New York", top_n=10):
    with get_conn() as conn:
        rows = conn.execute(f"""
            SELECT hour,
                   AVG(trip_count)    as avg_trips,
                   AVG(pulse_score)   as avg_pulse,
                   AVG(avg_fare)      as avg_fare,
                   AVG(total_revenue) as avg_revenue,
                   COUNT(*)           as data_points
            FROM agg_hourly_pulse
            WHERE city=?
            GROUP BY hour
            ORDER BY avg_pulse DESC
            LIMIT ?
        """, (city, top_n)).fetchall()
        return [dict(r) for r in rows]


def query_kpis(city="New York"):
    with get_conn() as conn:
        r = {}
        r["total_trips"]   = conn.execute("SELECT COUNT(*) FROM stg_taxi_trips WHERE city=?", (city,)).fetchone()[0]
        r["total_revenue"] = conn.execute("SELECT COALESCE(SUM(total_amount),0) FROM stg_taxi_trips WHERE city=?", (city,)).fetchone()[0]
        r["avg_fare"]      = conn.execute("SELECT COALESCE(AVG(fare_amount),0) FROM stg_taxi_trips WHERE city=?", (city,)).fetchone()[0]
        r["avg_speed"]     = conn.execute("SELECT COALESCE(AVG(speed_mph),0) FROM stg_taxi_trips WHERE city=? AND speed_mph > 0", (city,)).fetchone()[0]
        r["pipeline_runs"] = conn.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status='success' AND city=?", (city,)).fetchone()[0]
        r["last_run"]      = conn.execute(
            "SELECT finished_at FROM pipeline_runs WHERE status='success' AND city=? ORDER BY finished_at DESC LIMIT 1",
            (city,)
        ).fetchone()
        r["last_run"] = r["last_run"][0] if r["last_run"] else None

        bh = conn.execute("""
            SELECT hour, MAX(avg_trips) FROM (
                SELECT hour, AVG(trip_count) as avg_trips
                FROM agg_hourly_pulse WHERE city=? GROUP BY hour
            )
        """, (city,)).fetchone()
        r["busiest_hour"] = bh[0] if bh else None
        return r


def get_recent_trips(city="New York", limit=100):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM stg_taxi_trips WHERE city=?
            ORDER BY pickup_datetime DESC LIMIT ?
        """, (city, limit)).fetchall()
        return [dict(r) for r in rows]


def get_data_counts(city="New York"):
    with get_conn() as conn:
        return {
            "raw_trips":    conn.execute("SELECT COUNT(*) FROM raw_taxi_trips WHERE city=?", (city,)).fetchone()[0],
            "stg_trips":    conn.execute("SELECT COUNT(*) FROM stg_taxi_trips WHERE city=?", (city,)).fetchone()[0],
            "agg_hourly":   conn.execute("SELECT COUNT(*) FROM agg_hourly_pulse WHERE city=?", (city,)).fetchone()[0],
            "agg_daily":    conn.execute("SELECT COUNT(*) FROM agg_daily_summary WHERE city=?", (city,)).fetchone()[0],
            "agg_nbhd":     conn.execute("SELECT COUNT(*) FROM agg_neighborhood_stats WHERE city=?", (city,)).fetchone()[0],
            "weather_rows": conn.execute("SELECT COUNT(*) FROM stg_weather WHERE city=?", (city,)).fetchone()[0],
            "dq_checks":    conn.execute("SELECT COUNT(*) FROM data_quality_log").fetchone()[0],
            "pipeline_runs":conn.execute("SELECT COUNT(*) FROM pipeline_runs WHERE city=?", (city,)).fetchone()[0],
        }


def get_available_cities():
    """Return list of cities that have data in the warehouse."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT DISTINCT city, COUNT(*) as trip_count
            FROM stg_taxi_trips GROUP BY city ORDER BY trip_count DESC
        """).fetchall()
        return [dict(r) for r in rows]
