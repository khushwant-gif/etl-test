# weather_etl.py
import os
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

print("Starting Weather ETL process...")

# ----------------------------
# 1. Google Sheets Setup
# ----------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Check if service account file exists
if not os.path.exists("service_account.json") or os.path.getsize("service_account.json") == 0:
    print("Error: service_account.json file not found or empty!")
    exit(1)

try:
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    client = gspread.authorize(creds)
    print("✓ Successfully authorized with Google Sheets")
except Exception as e:
    print(f"Error with Google Sheets: {e}")
    exit(1)

# Open Google Sheet
SHEET_NAME = "Weather_Test_Data"
try:
    sheet = client.open(SHEET_NAME).sheet1
    print(f"✓ Successfully opened Google Sheet: {SHEET_NAME}")
except Exception as e:
    print(f"Error opening Google Sheet: {e}")
    exit(1)

# ----------------------------
# 2. Fetch Weather Data
# ----------------------------
print("Fetching weather data from Open-Meteo API...")
API_URL = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 52.52,           # Example: Berlin
    "longitude": 13.41,
    "hourly": ["temperature_2m","relative_humidity_2m","visibility","weather_code"],
    "timezone": "UTC"
}

try:
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    print("✓ Successfully fetched weather data")
except Exception as e:
    print(f"Error fetching weather data: {e}")
    exit(1)

# ----------------------------
# 3. Prepare DataFrame
# ----------------------------
hourly = data.get("hourly", {})
if not hourly:
    print("⚠️ No hourly data returned from API")
    exit(1)

df = pd.DataFrame({
    "Time": hourly.get("time", []),
    "Temperature_2m": hourly.get("temperature_2m", []),
    "Humidity_2m": hourly.get("relative_humidity_2m", []),
    "Visibility": hourly.get("visibility", []),
    "WeatherCode": hourly.get("weather_code", [])
})

df["Fetched_At"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
print(f"✓ Prepared DataFrame with {len(df)} rows")

# ----------------------------
# 4. Append Data to Google Sheet
# ----------------------------
# Check and add headers if sheet is empty
HEADERS = ["Time","Temperature_2m","Humidity_2m","Visibility","WeatherCode","Fetched_At"]
try:
    if sheet.row_count == 0 or len(sheet.row_values(1)) == 0:
        sheet.append_row(HEADERS)
        print("✓ Added headers to Google Sheet")
except Exception as e:
    print(f"Error adding headers: {e}")

# Append rows
rows = df.values.tolist()
try:
    if rows:
        sheet.append_rows(rows)
        print(f"✅ Successfully appended {len(rows)} rows to Google Sheet")
    else:
        print("⚠️ No data to append")
except Exception as e:
    print(f"Error uploading data to Google Sheet: {e}")
    exit(1)

print("\n🎉 Weather ETL process completed successfully!")
