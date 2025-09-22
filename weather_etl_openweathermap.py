import os
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

print("Starting Weather ETL process...")

# ----------------------------
# Google Sheets Setup
# ----------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)
sheet = client.open("Weather_Data").sheet1

# ----------------------------
# OpenWeatherMap Setup
# ----------------------------
API_KEY = os.environ.get("OPENWEATHER_API_KEY")
LAT = 52.52
LON = 13.41
EXCLUDE = "current,minutely,daily,alerts"
UNITS = "metric"

url = f"https://api.openweathermap.org/data/3.0/onecall?lat={LAT}&lon={LON}&exclude={EXCLUDE}&units={UNITS}&appid={API_KEY}"

response = requests.get(url, timeout=30)
data = response.json()
hourly = data.get("hourly", [])

print(f"Fetched {len(hourly)} hourly records")

# ----------------------------
# Prepare DataFrame
# ----------------------------
rows = []
for hour in hourly:
    rows.append([
        datetime.utcfromtimestamp(hour["dt"]).isoformat(),
        hour.get("temp"),
        hour.get("humidity"),
        hour.get("visibility"),
        hour.get("weather")[0]["id"] if hour.get("weather") else None,
        datetime.utcnow().isoformat()
    ])

headers = ["Time","Temperature_2m","Humidity_2m","Visibility","WeatherCode","Fetched_At"]

df = pd.DataFrame(rows, columns=headers)

# ----------------------------
# Update Google Sheet
# ----------------------------
try:
    if sheet.row_count == 0 or len(sheet.row_values(1)) == 0:
        sheet.append_row(headers)
    sheet.append_rows(df.values.tolist())
    print(f"✅ Uploaded {len(df)} rows to Google Sheet")
except Exception as e:
    print(f"Error updating Google Sheet: {e}")
