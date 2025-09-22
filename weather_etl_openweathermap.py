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
API_URL = "h_
