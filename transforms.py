"""
transforms.py — Pandas transformation pipeline
E → Extract, T → Transform, L → Load
City-aware: all transforms and aggregations carry the city field.
"""

import uuid
import json
from datetime import datetime

import pandas as pd
import numpy as np

from config import (
    CITY_PROFILES, DEFAULT_CITY,
    MAX_TRIP_DISTANCE_MILES, MAX_FARE_AMOUNT,
    MAX_TRIP_DURATION_MIN, MIN_PASSENGER_COUNT, MAX_PASSENGER_COUNT,
)
from database import (
    get_conn, log_dq_check, insert_pipeline_run, update_pipeline_run
)

DAYS        = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
SURGE_HOURS = set(range(7, 10)) | set(range(16, 20))


def _get_airports(city: str) -> set:
    profile = CITY_PROFILES.get(city, CITY_PROFILES[DEFAULT_CITY])
    return profile.get("airports", set())


# ── Data Quality Checks ────────────────────────────────────────────────────────
def run_dq_checks(df: pd.DataFrame, run_id: str, table: str) -> pd.DataFrame:
    original_count = len(df)

    nulls = df[["pickup_datetime","trip_distance_miles","fare_amount"]].isnull().sum()
    for col, n in nulls.items():
        log_dq_check(run_id, f"null_check_{col}", table,
                     "PASS" if n == 0 else "FAIL", 0, int(n))

    checks = [
        ("fare_range",     "fare_amount",        lambda d: (d >= 0) & (d <= MAX_FARE_AMOUNT)),
        ("distance_range", "trip_distance_miles", lambda d: (d >= 0) & (d <= MAX_TRIP_DISTANCE_MILES)),
        ("passenger_range","passenger_count",     lambda d: (d >= MIN_PASSENGER_COUNT) & (d <= MAX_PASSENGER_COUNT)),
        ("duration_range", "trip_duration_min",   lambda d: (d >= 0) & (d <= MAX_TRIP_DURATION_MIN)),
    ]
    for name, col, fn in checks:
        if col in df.columns:
            mask  = fn(df[col])
            fails = (~mask).sum()
            status = "PASS" if fails == 0 else ("WARN" if fails < len(df) * 0.05 else "FAIL")
            log_dq_check(run_id, name, table, status, "in range", f"{fails} out of range")
            df = df[mask]

    if "pickup_datetime" in df.columns:
        try:
            dt_series = pd.to_datetime(df["pickup_datetime"], errors="coerce")
            valid_dt  = (dt_series >= pd.Timestamp("2019-01-01")) & (dt_series <= pd.Timestamp.now())
            bad_dates = (~valid_dt).sum()
            log_dq_check(run_id, "date_range", table,
                         "PASS" if bad_dates == 0 else "WARN", "2019-now", f"{bad_dates} bad")
            df = df[valid_dt]
        except Exception:
            pass

    if "fare_amount" in df.columns and "payment_type" in df.columns:
        zero_fare = ((df["fare_amount"] == 0) & (df["payment_type"] != "No Charge")).sum()
        log_dq_check(run_id, "zero_fare", table,
                     "PASS" if zero_fare == 0 else "WARN", 0, int(zero_fare))

    completeness = round(len(df) / original_count * 100, 1) if original_count else 100
    log_dq_check(run_id, "completeness", table,
                 "PASS" if completeness >= 95 else "WARN",
                 "≥95%", f"{completeness}%")

    print(f"[DQ] {table}: {original_count} → {len(df)} rows "
          f"({original_count - len(df)} rejected). Completeness: {completeness}%")
    return df


# ── EXTRACT ────────────────────────────────────────────────────────────────────
def extract_trips(raw_trips: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(raw_trips)
    if df.empty:
        return df

    for col in ["fare_amount","tip_amount","total_amount",
                "trip_distance_miles","trip_duration_min","speed_mph"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "passenger_count" in df.columns:
        df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").fillna(1).astype(int)

    if "city" not in df.columns:
        df["city"] = DEFAULT_CITY

    return df


def extract_weather(raw_weather: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(raw_weather)
    if df.empty:
        return df
    for col in ["temp_f","feels_like_f","humidity_pct","wind_speed_mph","visibility_mi"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── TRANSFORM ──────────────────────────────────────────────────────────────────
def transform_trips(df: pd.DataFrame, run_id: str, city: str = DEFAULT_CITY) -> pd.DataFrame:
    if df.empty:
        return df

    print(f"[Transform] Processing {len(df)} raw trip records for {city} …")

    airports = _get_airports(city)

    df["pickup_dt"]  = pd.to_datetime(df["pickup_datetime"],  errors="coerce")
    df["dropoff_dt"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    df = df.dropna(subset=["pickup_dt"])

    df["pickup_hour"]         = df["pickup_dt"].dt.hour
    df["pickup_weekday"]      = df["pickup_dt"].dt.weekday
    df["pickup_weekday_name"] = df["pickup_dt"].dt.day_name()
    df["pickup_date"]         = df["pickup_dt"].dt.date.astype(str)

    mask = df["trip_duration_min"] <= 0
    if mask.any() and "dropoff_dt" in df.columns:
        delta = (df.loc[mask, "dropoff_dt"] - df.loc[mask, "pickup_dt"]).dt.total_seconds() / 60
        df.loc[mask, "trip_duration_min"] = delta.clip(lower=0)

    speed_mask = (df["speed_mph"] <= 0) & (df["trip_duration_min"] > 0)
    if speed_mask.any():
        df.loc[speed_mask, "speed_mph"] = (
            df.loc[speed_mask, "trip_distance_miles"] /
            (df.loc[speed_mask, "trip_duration_min"] / 60)
        ).round(2)
    df["speed_mph"] = df["speed_mph"].clip(0, 80)

    df["is_surge_hour"]   = df["pickup_hour"].isin(SURGE_HOURS).astype(int)
    df["is_weekend"]      = (df["pickup_weekday"] >= 5).astype(int)
    df["is_airport_trip"] = (
        df["pickup_neighborhood"].isin(airports) |
        df["dropoff_neighborhood"].isin(airports)
    ).astype(int)

    if "trip_id" not in df.columns:
        df["trip_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    payment_map = {"1":"Credit Card","2":"Cash","3":"No Charge","4":"Dispute"}
    df["payment_type"] = df["payment_type"].astype(str).map(
        lambda x: payment_map.get(x, x)
    )

    df["city"]                   = city
    df["pickup_neighborhood"]    = df["pickup_neighborhood"].fillna("Unknown")
    df["dropoff_neighborhood"]   = df["dropoff_neighborhood"].fillna("Unknown")

    df = run_dq_checks(df, run_id, "stg_taxi_trips")

    keep_cols = [
        "trip_id","city","pickup_datetime","dropoff_datetime","pickup_hour",
        "pickup_weekday","pickup_weekday_name","pickup_date",
        "pickup_neighborhood","dropoff_neighborhood","passenger_count",
        "trip_distance_miles","fare_amount","tip_amount","total_amount",
        "payment_type","trip_duration_min","speed_mph",
        "is_surge_hour","is_weekend","is_airport_trip",
    ]
    existing = [c for c in keep_cols if c in df.columns]
    df = df[existing].copy()

    print(f"[Transform] ✓ {len(df)} records after transformation")
    return df


def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["loaded_at"] = datetime.utcnow().isoformat()
    return df


# ── AGGREGATE ──────────────────────────────────────────────────────────────────
def compute_pulse_score(trip_count, avg_fare, avg_speed,
                        surge_pct, baseline_trips=500) -> float:
    trip_score  = min(trip_count / baseline_trips * 40, 40)
    fare_score  = min(avg_fare / 25 * 20, 20)
    speed_score = min(avg_speed / 20 * 20, 20) if avg_speed > 0 else 0
    surge_score = surge_pct * 20
    return round(min(trip_score + fare_score + speed_score + surge_score, 100), 2)


def aggregate_hourly_pulse(df: pd.DataFrame, city: str = DEFAULT_CITY) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    print(f"[Agg] Computing hourly pulse for {city} …")
    grp = df.groupby(["pickup_date","pickup_hour","pickup_weekday","pickup_weekday_name"])

    agg = grp.agg(
        trip_count      =("trip_id",            "count"),
        total_passengers=("passenger_count",     "sum"),
        avg_fare        =("fare_amount",         "mean"),
        avg_distance    =("trip_distance_miles", "mean"),
        avg_duration_min=("trip_duration_min",   "mean"),
        avg_speed_mph   =("speed_mph",           "mean"),
        total_revenue   =("total_amount",        "sum"),
        tip_avg         =("tip_amount",          "mean"),
        surge_trips     =("is_surge_hour",       "sum"),
        airport_trips   =("is_airport_trip",     "sum"),
    ).reset_index()

    agg.rename(columns={
        "pickup_date":"date", "pickup_hour":"hour",
        "pickup_weekday":"day_of_week", "pickup_weekday_name":"day_name"
    }, inplace=True)

    agg["city"]          = city
    agg["tip_rate_pct"]  = (agg["tip_avg"] / agg["avg_fare"].replace(0, float("nan")) * 100).fillna(0).round(2)
    agg["surge_trip_pct"]= (agg["surge_trips"]  / agg["trip_count"] * 100).round(2)
    agg["airport_pct"]   = (agg["airport_trips"] / agg["trip_count"] * 100).round(2)
    agg["pulse_score"]   = agg.apply(lambda r: compute_pulse_score(
        r["trip_count"], r["avg_fare"], r["avg_speed_mph"], r["surge_trip_pct"] / 100
    ), axis=1)
    agg["computed_at"]   = datetime.utcnow().isoformat()

    float_cols = ["avg_fare","avg_distance","avg_duration_min","avg_speed_mph",
                  "total_revenue","tip_rate_pct","surge_trip_pct","airport_pct","pulse_score"]
    for c in float_cols:
        agg[c] = agg[c].round(4)

    print(f"[Agg] ✓ {len(agg)} hourly pulse rows")
    return agg


def aggregate_neighborhood_stats(df: pd.DataFrame, city: str = DEFAULT_CITY) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    grp = df.groupby(["pickup_date","pickup_neighborhood"])
    agg = grp.agg(
        trip_count   =("trip_id",            "count"),
        avg_fare     =("fare_amount",         "mean"),
        total_revenue=("total_amount",        "sum"),
        avg_distance =("trip_distance_miles", "mean"),
    ).reset_index()
    agg.rename(columns={"pickup_date":"date","pickup_neighborhood":"neighborhood"}, inplace=True)
    agg["city"]          = city
    agg["rank_by_trips"] = agg.groupby("date")["trip_count"].rank(ascending=False, method="min").astype(int)
    agg["computed_at"]   = datetime.utcnow().isoformat()
    print(f"[Agg] ✓ {len(agg)} neighborhood rows")
    return agg


def aggregate_daily_summary(df: pd.DataFrame, city: str = DEFAULT_CITY) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    daily = df.groupby("pickup_date").agg(
        total_trips     =("trip_id",            "count"),
        total_passengers=("passenger_count",     "sum"),
        total_revenue   =("total_amount",        "sum"),
        avg_fare        =("fare_amount",         "mean"),
        avg_distance    =("trip_distance_miles", "mean"),
        avg_duration_min=("trip_duration_min",   "mean"),
    ).reset_index()

    hour_grp = df.groupby(["pickup_date","pickup_hour"])["trip_id"].count().reset_index()
    hour_grp.columns = ["pickup_date","hour","cnt"]
    busiest  = hour_grp.loc[hour_grp.groupby("pickup_date")["cnt"].idxmax()][["pickup_date","hour"]].rename(columns={"hour":"busiest_hour"})
    quietest = hour_grp.loc[hour_grp.groupby("pickup_date")["cnt"].idxmin()][["pickup_date","hour"]].rename(columns={"hour":"quietest_hour"})

    nb_day  = df.groupby(["pickup_date","pickup_neighborhood"])["trip_id"].count().reset_index()
    peak_nb = nb_day.loc[nb_day.groupby("pickup_date")["trip_id"].idxmax()][["pickup_date","pickup_neighborhood"]].rename(columns={"pickup_neighborhood":"peak_neighborhood"})

    daily = daily.merge(busiest,  on="pickup_date", how="left")
    daily = daily.merge(quietest, on="pickup_date", how="left")
    daily = daily.merge(peak_nb,  on="pickup_date", how="left")

    daily["city"]         = city
    daily["weekend_flag"] = pd.to_datetime(daily["pickup_date"]).dt.weekday.isin([5, 6]).astype(int)
    daily["computed_at"]  = datetime.utcnow().isoformat()
    daily.rename(columns={"pickup_date":"date"}, inplace=True)

    for c in ["avg_fare","avg_distance","avg_duration_min","total_revenue"]:
        daily[c] = daily[c].round(4)

    print(f"[Agg] ✓ {len(daily)} daily summary rows")
    return daily


# ── LOAD ───────────────────────────────────────────────────────────────────────
def load_raw(trips: list[dict], weather: list[dict], city: str = DEFAULT_CITY) -> int:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        for t in trips:
            conn.execute(
                "INSERT INTO raw_taxi_trips (ingested_at, city, source, raw_json) VALUES (?,?,?,?)",
                (now, city, "synthetic", json.dumps(t))
            )
        for w in weather:
            conn.execute(
                "INSERT INTO raw_weather (ingested_at, city, raw_json) VALUES (?,?,?)",
                (now, w.get("city", city), json.dumps(w))
            )
    return len(trips) + len(weather)


def load_staging_trips(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df["loaded_at"] = datetime.utcnow().isoformat()
    with get_conn() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO stg_taxi_trips
                    (trip_id, city, pickup_datetime, dropoff_datetime, pickup_hour,
                     pickup_weekday, pickup_weekday_name, pickup_date,
                     pickup_neighborhood, dropoff_neighborhood, passenger_count,
                     trip_distance_miles, fare_amount, tip_amount, total_amount,
                     payment_type, trip_duration_min, speed_mph,
                     is_surge_hour, is_weekend, is_airport_trip, loaded_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    row.get("trip_id"), row.get("city", DEFAULT_CITY),
                    row.get("pickup_datetime"), row.get("dropoff_datetime"),
                    int(row.get("pickup_hour", 0)), int(row.get("pickup_weekday", 0)),
                    row.get("pickup_weekday_name"), row.get("pickup_date"),
                    row.get("pickup_neighborhood"), row.get("dropoff_neighborhood"),
                    int(row.get("passenger_count", 1)),
                    float(row.get("trip_distance_miles", 0)),
                    float(row.get("fare_amount", 0)), float(row.get("tip_amount", 0)),
                    float(row.get("total_amount", 0)), row.get("payment_type"),
                    float(row.get("trip_duration_min", 0)), float(row.get("speed_mph", 0)),
                    int(row.get("is_surge_hour", 0)), int(row.get("is_weekend", 0)),
                    int(row.get("is_airport_trip", 0)), row.get("loaded_at"),
                ))
            except Exception:
                continue
    return len(df)


def load_staging_weather(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    with get_conn() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO stg_weather
                    (weather_id, recorded_at, city, temp_f, feels_like_f,
                     humidity_pct, wind_speed_mph, condition, condition_code,
                     visibility_mi, is_rain, is_snow, loaded_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    row.get("weather_id"), row.get("recorded_at"), row.get("city"),
                    row.get("temp_f"), row.get("feels_like_f"), row.get("humidity_pct"),
                    row.get("wind_speed_mph"), row.get("condition"), row.get("condition_code"),
                    row.get("visibility_mi"), int(row.get("is_rain", 0)),
                    int(row.get("is_snow", 0)), row.get("loaded_at"),
                ))
            except Exception:
                continue
    return len(df)


def load_hourly_pulse(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    with get_conn() as conn:
        for _, row in df.iterrows():
            conn.execute("""
                INSERT INTO agg_hourly_pulse
                  (city,date,hour,day_of_week,day_name,trip_count,total_passengers,
                   avg_fare,avg_distance,avg_duration_min,avg_speed_mph,
                   total_revenue,tip_rate_pct,surge_trip_pct,airport_pct,
                   pulse_score,computed_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(city,date,hour) DO UPDATE SET
                  trip_count=trip_count+excluded.trip_count,
                  total_passengers=total_passengers+excluded.total_passengers,
                  total_revenue=total_revenue+excluded.total_revenue,
                  avg_fare=(avg_fare+excluded.avg_fare)/2,
                  avg_distance=(avg_distance+excluded.avg_distance)/2,
                  avg_duration_min=(avg_duration_min+excluded.avg_duration_min)/2,
                  avg_speed_mph=(avg_speed_mph+excluded.avg_speed_mph)/2,
                  pulse_score=MAX(pulse_score,excluded.pulse_score),
                  computed_at=excluded.computed_at
            """, (
                row["city"], row["date"], int(row["hour"]),
                int(row["day_of_week"]), row["day_name"],
                int(row["trip_count"]), int(row["total_passengers"]),
                row["avg_fare"], row["avg_distance"], row["avg_duration_min"],
                row["avg_speed_mph"], row["total_revenue"], row["tip_rate_pct"],
                row["surge_trip_pct"], row["airport_pct"], row["pulse_score"],
                row["computed_at"]
            ))
    return len(df)


def load_neighborhood_stats(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    with get_conn() as conn:
        for _, row in df.iterrows():
            conn.execute("""
                INSERT INTO agg_neighborhood_stats
                  (city,date,neighborhood,trip_count,avg_fare,total_revenue,avg_distance,rank_by_trips,computed_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(city,date,neighborhood) DO UPDATE SET
                  trip_count=trip_count+excluded.trip_count,
                  total_revenue=total_revenue+excluded.total_revenue,
                  avg_fare=(avg_fare+excluded.avg_fare)/2,
                  computed_at=excluded.computed_at
            """, (
                row["city"], row["date"], row["neighborhood"],
                int(row["trip_count"]), row["avg_fare"], row["total_revenue"],
                row["avg_distance"], int(row["rank_by_trips"]), row["computed_at"]
            ))
    return len(df)


def load_daily_summary(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    with get_conn() as conn:
        for _, row in df.iterrows():
            conn.execute("""
                INSERT INTO agg_daily_summary
                  (city,date,total_trips,total_passengers,total_revenue,avg_fare,
                   avg_distance,avg_duration_min,busiest_hour,quietest_hour,
                   peak_neighborhood,weekend_flag,computed_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(city,date) DO UPDATE SET
                  total_trips=total_trips+excluded.total_trips,
                  total_revenue=total_revenue+excluded.total_revenue,
                  computed_at=excluded.computed_at
            """, (
                row["city"], row["date"],
                int(row["total_trips"]), int(row["total_passengers"]),
                row["total_revenue"], row["avg_fare"], row["avg_distance"],
                row["avg_duration_min"],
                int(row["busiest_hour"]) if pd.notna(row.get("busiest_hour")) else None,
                int(row["quietest_hour"]) if pd.notna(row.get("quietest_hour")) else None,
                row.get("peak_neighborhood"), int(row["weekend_flag"]),
                row["computed_at"]
            ))
    return len(df)


# ── Full ETL orchestrator ──────────────────────────────────────────────────────
def run_etl(raw_trips: list[dict], raw_weather: list[dict],
            city: str = DEFAULT_CITY,
            run_id: str = None, dag_id: str = "city_pulse") -> dict:
    if not run_id:
        run_id = str(uuid.uuid4())[:12]

    print(f"\n{'='*55}")
    print(f" ETL Run: {run_id}  |  City: {city}")
    print(f" Input: {len(raw_trips)} trips, {len(raw_weather)} weather readings")
    print(f"{'='*55}")

    stats = {
        "run_id": run_id, "city": city,
        "raw_trips": len(raw_trips), "raw_weather": len(raw_weather),
        "stg_trips_loaded": 0, "stg_weather_loaded": 0,
        "agg_hourly_rows": 0, "agg_nbhd_rows": 0, "agg_daily_rows": 0,
        "errors": [],
    }

    try:
        load_raw(raw_trips, raw_weather, city=city)
        print(f"[ETL] Raw layer: {len(raw_trips)} trips ingested")

        trips_df   = extract_trips(raw_trips)
        weather_df = extract_weather(raw_weather)

        trips_clean   = transform_trips(trips_df, run_id, city=city)
        weather_clean = transform_weather(weather_df)

        stats["stg_trips_loaded"]   = load_staging_trips(trips_clean)
        stats["stg_weather_loaded"] = load_staging_weather(weather_clean)
        print(f"[ETL] Staging: {stats['stg_trips_loaded']} trips, {stats['stg_weather_loaded']} weather")

        hourly_df = aggregate_hourly_pulse(trips_clean, city=city)
        nbhd_df   = aggregate_neighborhood_stats(trips_clean, city=city)
        daily_df  = aggregate_daily_summary(trips_clean, city=city)

        stats["agg_hourly_rows"] = load_hourly_pulse(hourly_df)
        stats["agg_nbhd_rows"]   = load_neighborhood_stats(nbhd_df)
        stats["agg_daily_rows"]  = load_daily_summary(daily_df)

        print(f"[ETL] Aggregations: {stats['agg_hourly_rows']} hourly, "
              f"{stats['agg_nbhd_rows']} nbhd, {stats['agg_daily_rows']} daily")
        print(f"[ETL] ✅ Run {run_id} complete\n")

    except Exception as e:
        stats["errors"].append(str(e))
        print(f"[ETL] ❌ Error: {e}")
        raise

    return stats
