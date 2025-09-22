import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime

print("🚀 Starting Weather ETL process...")

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "service_account.json"

creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open("Weather_Data").sheet1
print("✅ Connected to Google Sheets")

# Fixed coordinates
LAT = 28.61
LON = 77.23

# Fetch weather data from Open-Meteo
API_URL = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": LAT,
    "longitude": LON,
    "hourly": ["temperature_2m","relative_humidity_2m","visibility","weathercode"],
    "timezone": "Asia/Kolkata"
}

try:
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    print("🌍 Weather data fetched successfully")
except Exception as e:
    print(f"❌ Error fetching weather data: {e}")
    exit(1)

# Prepare data for Google Sheets
hourly = data["hourly"]
time = hourly["time"]
temperature = hourly["temperature_2m"]
humidity = hourly["relative_humidity_2m"]
visibility = hourly["visibility"]
weathercode = hourly["weathercode"]

df = pd.DataFrame({
    "Time": time,
    "Temperature_2m": temperature,
    "Humidity_2m": humidity,
    "Visibility": visibility,
    "WeatherCode": weathercode,
    "Fetched_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
})

# Check if sheet is empty and add headers
if sheet.row_count == 0 or len(sheet.row_values(1)) == 0:
    sheet.append_row(list(df.columns))
    
# Append data
sheet.append_rows(df.values.tolist())
print(f"✅ Successfully uploaded {len(df)} rows to Google Sheets")
print("🎉 ETL process completed!")
