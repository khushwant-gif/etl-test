import os
import base64
import json
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

print("🚀 Starting Weather ETL process...")

# ----------------------------
# 1. Google Sheets Setup
# ----------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Decode the service account JSON from GitHub Secrets
service_account_info = json.loads(base64.b64decode(os.getenv("SERVICE_ACCOUNT_B64")).decode("utf-8"))
creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
client = gspread.authorize(creds)
print("✅ Connected to Google Sheets")

sheet_name = "Weather_Data"
try:
    sheet = client.open(sheet_name).sheet1
except Exception as e:
    print(f"❌ Error opening Google Sheet: {e}")
    exit(1)

# ----------------------------
# 2. Open-Meteo Setup
# ----------------------------
LAT = float(os.getenv("LAT", "28.61"))
LON = float(os.getenv("LON", "77.23"))
API_URL = "https://api.open-meteo.com/v1/forecast"
EXCLUDE = "current,minutely,daily,alerts"
UNITS = "metric"
TIMEZONE = "Asia/Kolkata"

# ----------------------------
# 3. Helper Functions
# ----------------------------
def fetch_hourly_weather():
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": "temperature_2m,humidity_2m,visibility,weathercode",
        "timezone": TIMEZONE,
        "exclude": EXCLUDE,
        "units": UNITS
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json().get("hourly", [])

def format_row(hour_data):
    dt_iso = datetime.utcfromtimestamp(hour_data["timestamp"]).strftime("%Y-%m-%dT%H:%M")
    return [
        dt_iso,
        hour_data.get("temperature_2m", ""),
        hour_data.get("humidity_2m", ""),
        hour_data.get("visibility", ""),
        hour_data.get("weathercode", ""),
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    ]

# ----------------------------
# 4. Fetch Existing Times
# ----------------------------
existing_times = set(sheet.col_values(1)[1:])  # skip header

# ----------------------------
# 5. Backfill if Empty
# ----------------------------
rows_to_add = []

if not existing_times:
    print("⏳ Sheet empty. Performing 5-day historical backfill...")
    for days_ago in range(5, 0, -1):
        dt = int((datetime.utcnow() - timedelta(days=days_ago)).timestamp())
        try:
            hourly_data = fetch_hourly_weather()
            for h in hourly_data:
                row = format_row(h)
                if row[0] not in existing_times:
                    rows_to_add.append(row)
            print(f"   ✓ Fetched {len(hourly_data)} hours for {days_ago} days ago")
        except Exception as e:
            print(f"❌ Error fetching historical data: {e}")

# ----------------------------
# 6. Fetch Next 12 Hours Forecast
# ----------------------------
print("🌍 Fetching next 12 hours forecast...")
try:
    forecast_data = fetch_hourly_weather()
    for h in forecast_data[:12]:  # only next 12 hours
        row = format_row(h)
        if row[0] not in existing_times:
            rows_to_add.append(row)
    print(f"   ✓ Prepared {len(rows_to_add)} new rows for upload")
except Exception as e:
    print(f"❌ Error fetching weather data: {e}")
    exit(1)

# ----------------------------
# 7. Upload to Google Sheets
# ----------------------------
if rows_to_add:
    # Add headers if sheet is empty
    if sheet.row_count == 0 or len(sheet.row_values(1)) == 0:
        sheet.append_row(["Time", "Temperature", "Humidity", "Visibility", "WeatherCode", "Fetched_At"])
    sheet.append_rows(rows_to_add)
    print(f"✅ Successfully appended {len(rows_to_add)} rows to Google Sheets!")
else:
    print("⚠️ No new rows to add")

print("🎉 ETL process completed successfully!")
