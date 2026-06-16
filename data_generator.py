"""
data_generator.py — Synthetic NYC Taxi & Weather data generator
Produces statistically realistic data matching actual NYC TLC patterns.
Used when the real API/parquet is not reachable (offline mode).
Real data fetching is attempted first; this is the fallback.
"""

import random
import math
import uuid
import json
import requests
import os
from datetime import datetime, timedelta
from config import (
    NYC_NEIGHBORHOODS, TAXI_ROWS_PER_RUN,
    NYC_TAXI_PARQUET_URL, TAXI_SAMPLE_ENDPOINT,
    OPENWEATHER_KEY, OPENWEATHER_URL,
    MAX_TRIP_DISTANCE_MILES, MAX_FARE_AMOUNT,
    MAX_TRIP_DURATION_MIN, MIN_PASSENGER_COUNT, MAX_PASSENGER_COUNT
)

random.seed(42)

# ── Realistic NYC trip patterns by hour ───────────────────────────────────────
# Based on real TLC data: trips are heavy 7-10am, noon, 4-8pm, midnight
HOUR_WEIGHTS = {
    0: 0.60, 1: 0.45, 2: 0.35, 3: 0.20, 4: 0.15, 5: 0.18,
    6: 0.40, 7: 0.85, 8: 0.95, 9: 0.90, 10: 0.75, 11: 0.70,
    12: 0.80, 13: 0.75, 14: 0.70, 15: 0.75, 16: 0.90, 17: 1.00,
    18: 0.98, 19: 0.92, 20: 0.85, 21: 0.80, 22: 0.75, 23: 0.70,
}

WEEKDAY_MULTIPLIERS = {0:0.9, 1:0.92, 2:0.95, 3:0.95, 4:1.0, 5:1.1, 6:0.85}

NEIGHBORHOOD_WEIGHTS = {
    "Midtown":          0.20,
    "Times Square":     0.12,
    "Upper East Side":  0.10,
    "Financial District":0.08,
    "Hell's Kitchen":   0.07,
    "Chelsea":          0.07,
    "Upper West Side":  0.06,
    "Greenwich Village":0.05,
    "Tribeca":          0.05,
    "SoHo":             0.05,
    "Lower East Side":  0.04,
    "East Village":     0.03,
    "Murray Hill":      0.03,
    "Harlem":           0.02,
    "Gramercy":         0.03,
    "Williamsburg":     0.02,
    "Park Slope":       0.01,
    "Flushing":         0.01,
    "JFK Airport":      0.008,
    "LaGuardia Airport":0.006,
    "Astoria":          0.005,
    "Bronx Center":     0.004,
    "Jamaica":          0.003,
    "Staten Island":    0.001,
}

PAYMENT_TYPES = ["Credit Card", "Cash", "No Charge", "Dispute"]
PAYMENT_WEIGHTS = [0.72, 0.24, 0.02, 0.02]

WEATHER_CONDITIONS = [
    ("Clear", 800), ("Few Clouds", 801), ("Scattered Clouds", 802),
    ("Overcast", 804), ("Light Rain", 500), ("Moderate Rain", 501),
    ("Heavy Rain", 502), ("Drizzle", 300), ("Light Snow", 600),
    ("Snow", 601), ("Thunderstorm", 211), ("Mist", 701), ("Fog", 741),
]
WEATHER_WEIGHTS = [0.25, 0.15, 0.12, 0.10, 0.10, 0.06, 0.03,
                   0.05, 0.04, 0.02, 0.02, 0.04, 0.02]

AIRPORT_NEIGHBORHOODS = {"JFK Airport", "LaGuardia Airport"}


def _weighted_choice(choices: dict) -> str:
    keys   = list(choices.keys())
    weights = list(choices.values())
    return random.choices(keys, weights=weights, k=1)[0]


def _weighted_list(items: list, weights: list) -> object:
    return random.choices(items, weights=weights, k=1)[0]


def generate_trip(base_dt: datetime = None) -> dict:
    """Generate one realistic NYC taxi trip record."""
    if base_dt is None:
        # Random time in last 90 days
        base_dt = datetime.now() - timedelta(
            days=random.randint(0, 89),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

    weekday = base_dt.weekday()
    hour    = base_dt.hour

    # Apply hour and weekday weights for trip probability
    hw = HOUR_WEIGHTS.get(hour, 0.5)
    ww = WEEKDAY_MULTIPLIERS.get(weekday, 1.0)

    pickup_hood   = _weighted_choice(NEIGHBORHOOD_WEIGHTS)
    dropoff_hood  = _weighted_choice(NEIGHBORHOOD_WEIGHTS)

    # Distance: airport trips are longer
    is_airport = pickup_hood in AIRPORT_NEIGHBORHOODS or dropoff_hood in AIRPORT_NEIGHBORHOODS
    if is_airport:
        distance = round(random.uniform(8, 22), 2)
    else:
        distance = round(random.expovariate(1/3.5), 2)
        distance = min(distance, MAX_TRIP_DISTANCE_MILES)

    # Speed varies by hour (slower during rush)
    if hour in range(7, 10) or hour in range(16, 20):
        speed = round(random.uniform(8, 18), 1)   # rush hour — crawl
    elif hour in range(0, 5):
        speed = round(random.uniform(20, 35), 1)  # overnight — fast
    else:
        speed = round(random.uniform(14, 25), 1)

    duration_min = round((distance / speed) * 60, 1) if speed > 0 else 15
    duration_min = min(duration_min + random.uniform(1, 8), MAX_TRIP_DURATION_MIN)

    # Fare calculation (MTA-style: $3 base + $0.70/0.5mi + time)
    fare = 3.0 + (distance * 2.5) + (duration_min * 0.35)
    fare = round(min(fare + random.uniform(-1, 3), MAX_FARE_AMOUNT), 2)
    fare = max(fare, 3.0)

    # Tips higher for credit card, airport, higher fares
    payment = _weighted_list(PAYMENT_TYPES, PAYMENT_WEIGHTS)
    tip_pct = 0.0
    if payment == "Credit Card":
        tip_pct = random.choice([0.15, 0.18, 0.20, 0.22, 0.25, 0.30])
        if is_airport:
            tip_pct += 0.05
    tip = round(fare * tip_pct, 2)

    total = round(fare + tip + random.uniform(0.5, 2.5), 2)   # + tolls/fees

    passengers = random.choices(
        range(MIN_PASSENGER_COUNT, MAX_PASSENGER_COUNT + 1),
        weights=[0.55, 0.20, 0.12, 0.07, 0.04, 0.02], k=1
    )[0]

    dropoff_dt = base_dt + timedelta(minutes=duration_min)

    return {
        "trip_id":           str(uuid.uuid4()),
        "pickup_datetime":   base_dt.isoformat(),
        "dropoff_datetime":  dropoff_dt.isoformat(),
        "pickup_neighborhood": pickup_hood,
        "dropoff_neighborhood": dropoff_hood,
        "passenger_count":   passengers,
        "trip_distance_miles": distance,
        "fare_amount":       fare,
        "tip_amount":        tip,
        "total_amount":      total,
        "payment_type":      payment,
        "trip_duration_min": round(duration_min, 2),
        "speed_mph":         speed,
        "hour_weight":       hw,
        "weekday_mult":      ww,
    }


def generate_batch(n: int = TAXI_ROWS_PER_RUN,
                   start_dt: datetime = None,
                   span_days: int = 30) -> list[dict]:
    """Generate n trips spread across span_days days."""
    if start_dt is None:
        start_dt = datetime.now() - timedelta(days=span_days)

    trips = []
    for _ in range(n):
        offset_seconds = random.randint(0, span_days * 86400)
        base_dt = start_dt + timedelta(seconds=offset_seconds)
        trips.append(generate_trip(base_dt))
    return trips


def generate_weather_reading(city: str = "New York",
                              dt: datetime = None) -> dict:
    """Generate one realistic weather reading."""
    if dt is None:
        dt = datetime.now()

    month = dt.month
    # Seasonal temperature (New York)
    base_temp_f = {
        1: 35, 2: 38, 3: 48, 4: 60, 5: 70, 6: 80,
        7: 85, 8: 83, 9: 75, 10: 63, 11: 50, 12: 40
    }.get(month, 60)
    temp_f      = round(base_temp_f + random.uniform(-8, 8), 1)
    feels_like  = round(temp_f - random.uniform(2, 8), 1)

    condition, code = _weighted_list(WEATHER_CONDITIONS, WEATHER_WEIGHTS)
    humidity    = random.randint(40, 95)
    wind        = round(random.uniform(2, 18), 1)
    visibility  = round(random.uniform(4, 10), 1) if "Rain" in condition or "Fog" in condition else 10.0

    return {
        "weather_id":     str(uuid.uuid4()),
        "recorded_at":    dt.isoformat(),
        "city":           city,
        "temp_f":         temp_f,
        "feels_like_f":   feels_like,
        "humidity_pct":   humidity,
        "wind_speed_mph": wind,
        "condition":      condition,
        "condition_code": code,
        "visibility_mi":  visibility,
        "is_rain":        1 if "Rain" in condition or "Drizzle" in condition else 0,
        "is_snow":        1 if "Snow" in condition else 0,
    }


# ── Real data fetchers (with fallback) ────────────────────────────────────────
def fetch_real_taxi_sample(limit: int = 1000) -> list[dict]:
    """Try to fetch real NYC taxi data from Socrata API."""
    try:
        url    = f"{TAXI_SAMPLE_ENDPOINT}?$limit={limit}&$order=tpep_pickup_datetime+DESC"
        r      = requests.get(url, timeout=15)
        r.raise_for_status()
        rows   = r.json()
        result = []
        for row in rows:
            try:
                result.append({
                    "trip_id":             str(uuid.uuid4()),
                    "pickup_datetime":     row.get("tpep_pickup_datetime",""),
                    "dropoff_datetime":    row.get("tpep_dropoff_datetime",""),
                    "pickup_neighborhood": row.get("pulocationid","Unknown"),
                    "dropoff_neighborhood":row.get("dolocationid","Unknown"),
                    "passenger_count":     int(row.get("passenger_count",1) or 1),
                    "trip_distance_miles": float(row.get("trip_distance",0) or 0),
                    "fare_amount":         float(row.get("fare_amount",0) or 0),
                    "tip_amount":          float(row.get("tip_amount",0) or 0),
                    "total_amount":        float(row.get("total_amount",0) or 0),
                    "payment_type":        row.get("payment_type","1"),
                    "trip_duration_min":   0,
                    "speed_mph":           0,
                })
            except (ValueError, TypeError):
                continue
        return result
    except Exception as e:
        print(f"[DataGen] Real API unavailable ({e}) — using synthetic data")
        return []


def fetch_real_weather(city: str = "New York") -> dict | None:
    """Try to fetch real weather from OpenWeather API."""
    if not OPENWEATHER_KEY:
        return None
    try:
        r = requests.get(OPENWEATHER_URL, params={
            "q": city, "appid": OPENWEATHER_KEY, "units": "imperial"
        }, timeout=10)
        r.raise_for_status()
        data = r.json()
        return {
            "weather_id":     str(uuid.uuid4()),
            "recorded_at":    datetime.utcnow().isoformat(),
            "city":           city,
            "temp_f":         data["main"]["temp"],
            "feels_like_f":   data["main"]["feels_like"],
            "humidity_pct":   data["main"]["humidity"],
            "wind_speed_mph": data["wind"]["speed"],
            "condition":      data["weather"][0]["description"].title(),
            "condition_code": data["weather"][0]["id"],
            "visibility_mi":  round(data.get("visibility",10000)/1609.34, 1),
            "is_rain":        1 if data["weather"][0]["id"] in range(300,600) else 0,
            "is_snow":        1 if data["weather"][0]["id"] in range(600,700) else 0,
        }
    except Exception as e:
        print(f"[DataGen] OpenWeather unavailable ({e}) — using synthetic data")
        return None
