# weather_etl.py
import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

print("🚀 Starting Weather ETL process...")

# ----------------------------
# 1. Google Sheets Setup
# ----------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

if not os.path.exists("service_account.json"):
    print("❌ Error: service_account.json file not found!")
    exit(1)

creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)
print("✅ Connected to Google Sheets")

sheet_name = "Weather_Data"
try:
    sheet = client.open(sheet_name).sheet1
except Exception as e:
    print(f"❌ Error opening Google Sheet: {e}")
    exit(1)

# ----------------------------
# 2. OpenWeather Setup
# ----------------------------
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    print("❌ OPENWEATHER_API_KEY not found in environment variables")
    exit(1)

LAT = 28.61    # Example: Delhi latitude
LON = 77.23    # Example: Delhi longitude
EXCLUDE = "minutely,daily,alerts,current"
UNITS = "metric"

API_URL = f"https://api.openweathermap.org/data/2.5/onecall"

# ----------------------------
# 3. Helper Functions
# ----------------------------
def fetch_hourly_weather(dt=None):
    """
    Fetch hourly weather. If dt is None, fetch current forecast (48h).
    If dt is timestamp, fetch historical hourly data for that day.
    """
    params = {
        "lat": LAT,
        "lon": LON,
        "appid": API_KEY,
        "units": UNITS,
        "exclude": EXCLUDE
    }
    if dt:
        # Historical API endpoint
        url = f"https://api.openweathermap.org/data/2.5/onecall/timemachine"
        params["dt"] = dt
    else:
        url = API_URL

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json().get("hourly", [])

def format_row(hour_data):
    dt_iso = datetime.utcfromtimestamp(hour_data["dt"]).strftime("%Y-%m-%dT%H:%M")
    return [
        dt_iso,
        hour_data.get("temp", ""),
        hour_data.get("humidity", ""),
        hour_data.get("visibility", ""),
        hour_data.get("weather")[0]["id"] if "weather" in hour_data else "",
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
            hourly_data = fetch_hourly_weather(dt)
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
