"""
config.py — Central configuration for City Pulse ETL Pipeline
All paths, constants, and settings live here.
"""

import os

# ── Base paths ─────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(BASE_DIR, "data")
DAGS_DIR  = os.path.join(BASE_DIR, "dags")
DB_PATH   = os.path.join(DATA_DIR, "city_pulse.db")
LOG_PATH  = os.path.join(DATA_DIR, "pipeline.log")
RAW_DIR   = os.path.join(DATA_DIR, "raw")
PROC_DIR  = os.path.join(DATA_DIR, "processed")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR,  exist_ok=True)
os.makedirs(PROC_DIR, exist_ok=True)

# ── City Profiles ──────────────────────────────────────────────────────────────
# Each city has its own neighborhoods, fare structure, airports, and currency.
CITY_PROFILES = {
    "New York": {
        "display_name":   "New York City",
        "country":        "USA",
        "currency":       "$",
        "currency_code":  "USD",
        "weather_query":  "New York",
        "timezone":       "America/New_York",
        "base_fare":      3.00,
        "per_mile_rate":  2.50,
        "per_min_rate":   0.35,
        "speed_day":      (14, 25),
        "speed_rush":     (8, 18),
        "speed_night":    (20, 35),
        "airports": {"JFK Airport", "LaGuardia Airport", "Newark Airport"},
        "neighborhoods": [
            "Midtown", "Times Square", "Upper East Side", "Upper West Side",
            "Lower East Side", "Greenwich Village", "Chelsea", "Harlem",
            "Financial District", "Hell's Kitchen", "Tribeca", "SoHo",
            "East Village", "Murray Hill", "Gramercy", "Astoria",
            "Williamsburg", "Park Slope", "Flushing", "Jamaica",
            "Bronx Center", "Staten Island", "JFK Airport", "LaGuardia Airport",
        ],
        "neighborhood_weights": {
            "Midtown": 0.20, "Times Square": 0.12, "Upper East Side": 0.10,
            "Financial District": 0.08, "Hell's Kitchen": 0.07, "Chelsea": 0.07,
            "Upper West Side": 0.06, "Greenwich Village": 0.05, "Tribeca": 0.05,
            "SoHo": 0.05, "Lower East Side": 0.04, "East Village": 0.03,
            "Murray Hill": 0.03, "Harlem": 0.02, "Gramercy": 0.03,
            "Williamsburg": 0.02, "Park Slope": 0.01, "Flushing": 0.01,
            "JFK Airport": 0.008, "LaGuardia Airport": 0.006,
            "Astoria": 0.005, "Bronx Center": 0.004,
            "Jamaica": 0.003, "Staten Island": 0.001,
        },
        "seasonal_temps": {
            1:35,2:38,3:48,4:60,5:70,6:80,
            7:85,8:83,9:75,10:63,11:50,12:40
        },
    },

    "London": {
        "display_name":   "London",
        "country":        "UK",
        "currency":       "£",
        "currency_code":  "GBP",
        "weather_query":  "London",
        "timezone":       "Europe/London",
        "base_fare":      2.60,
        "per_mile_rate":  2.00,
        "per_min_rate":   0.40,
        "speed_day":      (10, 20),
        "speed_rush":     (5, 12),
        "speed_night":    (18, 30),
        "airports": {"Heathrow Airport", "Gatwick Airport", "London City Airport"},
        "neighborhoods": [
            "City of London", "Westminster", "Canary Wharf", "Shoreditch",
            "Soho", "Covent Garden", "Mayfair", "Notting Hill",
            "Chelsea", "Brixton", "Camden", "Islington",
            "Greenwich", "Hackney", "Peckham", "Stratford",
            "Heathrow Airport", "Gatwick Airport", "London City Airport",
            "Croydon", "Wimbledon", "Richmond",
        ],
        "neighborhood_weights": {
            "City of London": 0.15, "Westminster": 0.12, "Canary Wharf": 0.10,
            "Shoreditch": 0.08, "Soho": 0.08, "Covent Garden": 0.07,
            "Mayfair": 0.06, "Notting Hill": 0.05, "Chelsea": 0.05,
            "Brixton": 0.04, "Camden": 0.04, "Islington": 0.03,
            "Greenwich": 0.03, "Hackney": 0.03, "Peckham": 0.02,
            "Stratford": 0.02, "Heathrow Airport": 0.01, "Gatwick Airport": 0.008,
            "London City Airport": 0.006, "Croydon": 0.005,
            "Wimbledon": 0.004, "Richmond": 0.003,
        },
        "seasonal_temps": {
            1:40,2:42,3:48,4:55,5:62,6:68,
            7:72,8:71,9:65,10:56,11:48,12:42
        },
    },

    "Mumbai": {
        "display_name":   "Mumbai",
        "country":        "India",
        "currency":       "₹",
        "currency_code":  "INR",
        "weather_query":  "Mumbai",
        "timezone":       "Asia/Kolkata",
        "base_fare":      21.00,
        "per_mile_rate":  14.00,
        "per_min_rate":   1.50,
        "speed_day":      (8, 18),
        "speed_rush":     (4, 10),
        "speed_night":    (18, 30),
        "airports": {"Chhatrapati Shivaji Airport", "Juhu Aerodrome"},
        "neighborhoods": [
            "Colaba", "Nariman Point", "Fort", "Dadar",
            "Bandra", "Andheri", "Juhu", "Powai",
            "Worli", "Lower Parel", "Kurla", "Thane",
            "Borivali", "Malad", "Goregaon", "Kandivali",
            "Chhatrapati Shivaji Airport", "Juhu Aerodrome",
            "Navi Mumbai", "Dharavi", "Mahim", "Versova",
        ],
        "neighborhood_weights": {
            "Colaba": 0.10, "Nariman Point": 0.09, "Fort": 0.08,
            "Bandra": 0.10, "Andheri": 0.09, "Juhu": 0.06,
            "Powai": 0.06, "Worli": 0.06, "Lower Parel": 0.07,
            "Kurla": 0.05, "Dadar": 0.05, "Thane": 0.04,
            "Borivali": 0.03, "Malad": 0.03, "Goregaon": 0.03,
            "Kandivali": 0.02, "Chhatrapati Shivaji Airport": 0.01,
            "Juhu Aerodrome": 0.005, "Navi Mumbai": 0.005,
            "Dharavi": 0.003, "Mahim": 0.003, "Versova": 0.002,
        },
        "seasonal_temps": {
            1:75,2:78,3:84,4:89,5:91,6:84,
            7:80,8:80,9:81,10:84,11:82,12:77
        },
    },

    "Chicago": {
        "display_name":   "Chicago",
        "country":        "USA",
        "currency":       "$",
        "currency_code":  "USD",
        "weather_query":  "Chicago",
        "timezone":       "America/Chicago",
        "base_fare":      3.25,
        "per_mile_rate":  2.25,
        "per_min_rate":   0.30,
        "speed_day":      (14, 26),
        "speed_rush":     (8, 16),
        "speed_night":    (22, 38),
        "airports": {"O'Hare Airport", "Midway Airport"},
        "neighborhoods": [
            "The Loop", "River North", "Gold Coast", "Lincoln Park",
            "Wicker Park", "Wrigleyville", "Hyde Park", "Pilsen",
            "Andersonville", "Logan Square", "Bucktown", "South Loop",
            "West Loop", "Streeterville", "Ukrainian Village",
            "Rogers Park", "Bronzeville", "Chinatown",
            "O'Hare Airport", "Midway Airport",
        ],
        "neighborhood_weights": {
            "The Loop": 0.18, "River North": 0.12, "Gold Coast": 0.10,
            "Lincoln Park": 0.09, "Wicker Park": 0.07, "Wrigleyville": 0.06,
            "Hyde Park": 0.05, "Pilsen": 0.05, "Andersonville": 0.04,
            "Logan Square": 0.05, "Bucktown": 0.04, "South Loop": 0.04,
            "West Loop": 0.05, "Streeterville": 0.04, "Ukrainian Village": 0.03,
            "Rogers Park": 0.02, "Bronzeville": 0.02, "Chinatown": 0.02,
            "O'Hare Airport": 0.015, "Midway Airport": 0.01,
        },
        "seasonal_temps": {
            1:24,2:28,3:38,4:50,5:61,6:71,
            7:76,8:75,9:67,10:55,11:41,12:29
        },
    },

    "Tokyo": {
        "display_name":   "Tokyo",
        "country":        "Japan",
        "currency":       "¥",
        "currency_code":  "JPY",
        "weather_query":  "Tokyo",
        "timezone":       "Asia/Tokyo",
        "base_fare":      410.00,
        "per_mile_rate":  300.00,
        "per_min_rate":   80.00,
        "speed_day":      (12, 22),
        "speed_rush":     (6, 14),
        "speed_night":    (18, 32),
        "airports": {"Narita Airport", "Haneda Airport"},
        "neighborhoods": [
            "Shinjuku", "Shibuya", "Ginza", "Akihabara",
            "Roppongi", "Harajuku", "Asakusa", "Ueno",
            "Ikebukuro", "Akasaka", "Odaiba", "Shimokitazawa",
            "Nakameguro", "Daikanyama", "Marunouchi", "Ebisu",
            "Narita Airport", "Haneda Airport",
            "Shinagawa", "Yurakucho",
        ],
        "neighborhood_weights": {
            "Shinjuku": 0.15, "Shibuya": 0.13, "Ginza": 0.10,
            "Akihabara": 0.07, "Roppongi": 0.08, "Harajuku": 0.07,
            "Asakusa": 0.06, "Ueno": 0.06, "Ikebukuro": 0.07,
            "Akasaka": 0.05, "Odaiba": 0.04, "Shimokitazawa": 0.03,
            "Nakameguro": 0.03, "Daikanyama": 0.03, "Marunouchi": 0.04,
            "Ebisu": 0.03, "Narita Airport": 0.01, "Haneda Airport": 0.01,
            "Shinagawa": 0.03, "Yurakucho": 0.02,
        },
        "seasonal_temps": {
            1:38,2:40,3:50,4:60,5:68,6:74,
            7:82,8:84,9:76,10:65,11:54,12:44
        },
    },

    "Dubai": {
        "display_name":   "Dubai",
        "country":        "UAE",
        "currency":       "AED",
        "currency_code":  "AED",
        "weather_query":  "Dubai",
        "timezone":       "Asia/Dubai",
        "base_fare":      5.00,
        "per_mile_rate":  2.80,
        "per_min_rate":   0.50,
        "speed_day":      (20, 40),
        "speed_rush":     (10, 22),
        "speed_night":    (35, 60),
        "airports": {"Dubai International Airport", "Al Maktoum Airport"},
        "neighborhoods": [
            "Downtown Dubai", "Dubai Marina", "Palm Jumeirah", "Deira",
            "Bur Dubai", "Jumeirah", "Business Bay", "DIFC",
            "JBR", "Silicon Oasis", "Sports City", "Motor City",
            "Discovery Gardens", "International City", "Karama",
            "Dubai International Airport", "Al Maktoum Airport",
            "Al Barsha", "Mirdif", "Rashidiya",
        ],
        "neighborhood_weights": {
            "Downtown Dubai": 0.16, "Dubai Marina": 0.13, "Palm Jumeirah": 0.09,
            "Deira": 0.08, "Bur Dubai": 0.07, "Jumeirah": 0.07,
            "Business Bay": 0.08, "DIFC": 0.06, "JBR": 0.05,
            "Silicon Oasis": 0.03, "Sports City": 0.03, "Motor City": 0.02,
            "Discovery Gardens": 0.03, "International City": 0.02, "Karama": 0.03,
            "Dubai International Airport": 0.02, "Al Maktoum Airport": 0.01,
            "Al Barsha": 0.03, "Mirdif": 0.02, "Rashidiya": 0.01,
        },
        "seasonal_temps": {
            1:65,2:68,3:74,4:82,5:92,6:100,
            7:106,8:106,9:100,10:92,11:80,12:70
        },
    },

    "Paris": {
        "display_name":   "Paris",
        "country":        "France",
        "currency":       "€",
        "currency_code":  "EUR",
        "weather_query":  "Paris",
        "timezone":       "Europe/Paris",
        "base_fare":      2.50,
        "per_mile_rate":  1.80,
        "per_min_rate":   0.38,
        "speed_day":      (12, 22),
        "speed_rush":     (6, 14),
        "speed_night":    (18, 30),
        "airports": {"Charles de Gaulle Airport", "Orly Airport", "Beauvais Airport"},
        "neighborhoods": [
            "Le Marais", "Saint-Germain-des-Prés", "Montmartre", "Champs-Élysées",
            "Bastille", "Belleville", "Pigalle", "Opéra",
            "La Défense", "Châtelet", "Oberkampf", "République",
            "Père Lachaise", "Nation", "Vincennes", "Boulogne",
            "Charles de Gaulle Airport", "Orly Airport",
            "Neuilly-sur-Seine", "Versailles",
        ],
        "neighborhood_weights": {
            "Le Marais": 0.12, "Saint-Germain-des-Prés": 0.10, "Montmartre": 0.09,
            "Champs-Élysées": 0.10, "Bastille": 0.07, "Belleville": 0.05,
            "Pigalle": 0.06, "Opéra": 0.07, "La Défense": 0.07,
            "Châtelet": 0.06, "Oberkampf": 0.04, "République": 0.05,
            "Père Lachaise": 0.03, "Nation": 0.03, "Vincennes": 0.02,
            "Boulogne": 0.02, "Charles de Gaulle Airport": 0.015,
            "Orly Airport": 0.01, "Neuilly-sur-Seine": 0.02, "Versailles": 0.01,
        },
        "seasonal_temps": {
            1:38,2:40,3:48,4:56,5:64,6:70,
            7:76,8:75,9:67,10:56,11:46,12:39
        },
    },

    "Sydney": {
        "display_name":   "Sydney",
        "country":        "Australia",
        "currency":       "A$",
        "currency_code":  "AUD",
        "weather_query":  "Sydney",
        "timezone":       "Australia/Sydney",
        "base_fare":      3.60,
        "per_mile_rate":  2.20,
        "per_min_rate":   0.55,
        "speed_day":      (14, 26),
        "speed_rush":     (8, 16),
        "speed_night":    (22, 38),
        "airports": {"Sydney Airport", "Bankstown Airport"},
        "neighborhoods": [
            "CBD", "The Rocks", "Darling Harbour", "Surry Hills",
            "Newtown", "Glebe", "Bondi", "Manly",
            "Chatswood", "Parramatta", "Liverpool", "Penrith",
            "Cronulla", "Mosman", "Balmain", "Leichhardt",
            "Sydney Airport", "Bankstown Airport",
            "North Sydney", "Darlinghurst",
        ],
        "neighborhood_weights": {
            "CBD": 0.16, "The Rocks": 0.08, "Darling Harbour": 0.09,
            "Surry Hills": 0.07, "Newtown": 0.06, "Glebe": 0.05,
            "Bondi": 0.08, "Manly": 0.05, "Chatswood": 0.05,
            "Parramatta": 0.05, "Liverpool": 0.03, "Penrith": 0.02,
            "Cronulla": 0.03, "Mosman": 0.04, "Balmain": 0.04,
            "Leichhardt": 0.03, "Sydney Airport": 0.015,
            "Bankstown Airport": 0.005, "North Sydney": 0.05,
            "Darlinghurst": 0.04,
        },
        "seasonal_temps": {
            1:73,2:73,3:69,4:63,5:57,6:52,
            7:50,8:53,9:58,10:64,11:69,12:73
        },
    },
}

SUPPORTED_CITIES = list(CITY_PROFILES.keys())
DEFAULT_CITY     = "New York"

# ── Data sources ───────────────────────────────────────────────────────────────
NYC_TAXI_PARQUET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)
NYC_OPEN_DATA_BASE   = "https://data.cityofnewyork.us/resource"
TAXI_SAMPLE_ENDPOINT = f"{NYC_OPEN_DATA_BASE}/gkne-dk5s.json"

# OpenWeather API
OPENWEATHER_KEY  = os.environ.get("OPENWEATHER_API_KEY", "")
OPENWEATHER_URL  = "https://api.openweathermap.org/data/2.5/weather"

# ── Pipeline schedule ──────────────────────────────────────────────────────────
SCHEDULE_INTERVAL_MIN = 60
TAXI_ROWS_PER_RUN     = 50_000

# ── Data quality thresholds ────────────────────────────────────────────────────
MAX_TRIP_DISTANCE_MILES = 100
MAX_FARE_AMOUNT         = 5000    # raised to accommodate JPY/INR fares
MAX_TRIP_DURATION_MIN   = 180
MIN_PASSENGER_COUNT     = 1
MAX_PASSENGER_COUNT     = 6

# ── Dashboard ──────────────────────────────────────────────────────────────────
DASHBOARD_PORT = 5050
DASHBOARD_HOST = "0.0.0.0"

# ── Airflow-like settings ──────────────────────────────────────────────────────
AIRFLOW_HOME = os.path.join(BASE_DIR, "airflow_home")
os.makedirs(AIRFLOW_HOME, exist_ok=True)
