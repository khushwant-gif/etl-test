import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

print("🚀 Starting Weather ETL process...")

# ----------------------------
# 1. Google Sheets Setup
# ----------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

if not os.path.exists("service_account.json"):
    print("❌ service_account.json not found")
    exit(1)

try:
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open("Weather_Data").sheet1
    print("✅ Connected to Google Sheets")
except Exception as e:
    print(f"❌ Error connecting to Google Sheets: {e}")
    exit(1)

# ----------------------------
# 2. OpenWeather API Setup
# ----------------------------
API_KEY = os.getenv("OPENWEATHER_API_KEY")  # <-- GitHub Secret

if not API_KEY:
    print("❌ OPENWEATHER_API_KEY not found in environment variables")
    exit(1)

LAT, LON = 28.61, 77.23  # Example: Delhi, change to your location

API_URL = (
    f"https://api.openweathermap.org/data/3.0/onecall"
    f"?lat={LAT}&lon={LON}&exclude=minutely,daily,alerts,current"
    f"&appid={API_KEY}&units=metric"
)

print("🌍 Fetching weather data...")
try:
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    print("✅ Weather data fetched")
except Exception as e:
    print(f"❌ Error fetching weather data: {e}")
    exit(1)

# ----------------------------
# 3. Prepare Data
# ----------------------------
headers = ["Time", "Temperature_2m", "Humidity_2m", "Visibility", "WeatherCode", "Fetched_At"]

# Add headers if sheet is empty
if len(sheet.get_all_values()) == 0:
    sheet.append_row(headers)
    print("📝 Added headers to sheet")

# Collect already present times
existing_times = [row[0] for row in sheet.get_all_values()[1:]]  # skip header

rows_to_add = []
for hourly in data.get("hourly", [])[:12]:  # next 12 hours
    time_str = datetime.utcfromtimestamp(hourly["dt"]).strftime("%Y-%m-%dT%H:%M")
    if time_str not in existing_times:
        row = [
            time_str,
            hourly.get("temp", ""),
            hourly.get("humidity", ""),
            hourly.get("visibility", ""),
            hourly["weather"][0]["id"] if "weather" in hourly else "",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        ]
        rows_to_add.append(row)

print(f"📊 Prepared {len(rows_to_add)} new rows")

# ----------------------------
# 4. Upload Data
# ----------------------------
if rows_to_add:
    try:
        sheet.append_rows(rows_to_add)
        print(f"✅ Added {len(rows_to_add)} new rows to Google Sheets")
    except Exception as e:
        print(f"❌ Error uploading to Google Sheets: {e}")
else:
    print("⏩ No new rows to add (all data already exists)")

print("🎉 Weather ETL completed successfully")
