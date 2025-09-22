import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import logging
import time
import json

# ----------------------------
# Logging configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("weather_etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ----------------------------
# Weather ETL Class
# ----------------------------
class WeatherETL:
    def __init__(self, service_account_file="service_account.json",
                 sheet_name="Weather_Data", lat=28.61, lon=77.23):
        self.SERVICE_ACCOUNT_FILE = service_account_file
        self.SHEET_NAME = sheet_name
        self.LAT = lat
        self.LON = lon
        self.client = None
        self.sheet = None
        self.state_file = "etl_state.json"

    # ----------------------------
    # State handling
    # ----------------------------
    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    logger.info(f"📁 Loaded state: {state}")
                    return state
            except Exception as e:
                logger.warning(f"⚠️ Failed to load state: {e}")
        return {"first_run": True, "last_run": None}

    def save_state(self, state):
        try:
            with open(self.state_file, "w") as f:
                json.dump(state, f)
            logger.info(f"💾 Saved state: {state}")
        except Exception as e:
            logger.error(f"❌ Error saving state: {e}")

    # ----------------------------
    # Google Sheets Setup
    # ----------------------------
    def setup_google_sheets(self):
        try:
            if not os.path.exists(self.SERVICE_ACCOUNT_FILE):
                raise FileNotFoundError(f"Service account file missing: {self.SERVICE_ACCOUNT_FILE}")

            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.SERVICE_ACCOUNT_FILE, scope)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open(self.SHEET_NAME).sheet1
            logger.info("✅ Connected to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"❌ Google Sheets connection error: {e}")
            return False

    def get_existing_timestamps(self):
        try:
            rows = self.sheet.get_all_values()
            if not rows or len(rows) <= 1:
                return set()
            timestamps = set(row[0] for row in rows[1:] if row)
            logger.info(f"📋 Found {len(timestamps)} existing timestamps")
            return timestamps
        except Exception as e:
            logger.error(f"❌ Failed to read existing timestamps: {e}")
            return set()

    # ----------------------------
    # Fetch weather data
    # ----------------------------
    def fetch_weather_data(self, start_date=None, end_date=None, forecast_hours=12):
        if start_date and end_date:
            # Historical month data
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": self.LAT,
                "longitude": self.LON,
                "hourly": ["temperature_2m", "relative_humidity_2m", "visibility", "weathercode", "precipitation"],
                "timezone": "Asia/Kolkata",
                "start_date": start_date,
                "end_date": end_date
            }
            logger.info(f"📅 Fetching historical data: {start_date} to {end_date}")
        else:
            # Forecast next 12 hours
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": self.LAT,
                "longitude": self.LON,
                "hourly": ["temperature_2m", "relative_humidity_2m", "visibility", "weathercode", "precipitation"],
                "timezone": "Asia/Kolkata"
            }
            logger.info(f"🔮 Fetching next {forecast_hours} hours forecast")

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if "hourly" not in data or not data["hourly"].get("time"):
                logger.warning("⚠️ API response missing 'hourly' data")
                return None
            return data
        except Exception as e:
            logger.error(f"❌ Failed to fetch weather data: {e}")
            return None

    # ----------------------------
    # Prepare rows for Sheets
    # ----------------------------
    def prepare_rows(self, data, existing_timestamps=None):
        existing_timestamps = existing_timestamps or set()
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        temp = hourly.get("temperature_2m", [])
        humidity = hourly.get("relative_humidity_2m", [])
        vis = hourly.get("visibility", [])
        code = hourly.get("weathercode", [])
        precip = hourly.get("precipitation", [])

        rows = []
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i, t in enumerate(times):
            if t in existing_timestamps:
                continue
            row = [
                t,
                temp[i] if i < len(temp) else "",
                humidity[i] if i < len(humidity) else "",
                vis[i] if i < len(vis) else "",
                code[i] if i < len(code) else "",
                precip[i] if i < len(precip) else "",
                fetched_at
            ]
            rows.append(row)

        logger.info(f"📊 Prepared {len(rows)} new rows")
        return rows

    # ----------------------------
    # Upload to Google Sheets
    # ----------------------------
    def upload_to_sheets(self, rows):
        if not rows:
            logger.info("📋 No new data to upload")
            return True

        # Add headers if sheet empty
        if self.sheet.row_count == 0 or len(self.sheet.row_values(1)) == 0:
            headers = ["Time", "Temperature_2m", "Humidity_2m", "Visibility", "WeatherCode", "Precipitation", "Fetched_At"]
            self.sheet.append_row(headers)
            logger.info("📋 Headers added")

        batch_size = 100
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            self.sheet.append_rows(batch)
            time.sleep(1)
        logger.info(f"✅ Uploaded {len(rows)} rows to Google Sheets")
        return True

    # ----------------------------
    # Run ETL
    # ----------------------------
    def run(self):
        logger.info("🚀 Starting Weather ETL process...")
        state = self.load_state()

        if not self.setup_google_sheets():
            return False

        success = False

        if state.get("first_run", True):
            # Previous month
            today = datetime.now().date()
            first_day_prev_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
            last_day_prev_month = today.replace(day=1) - timedelta(days=1)

            data = self.fetch_weather_data(
                start_date=first_day_prev_month.strftime("%Y-%m-%d"),
                end_date=last_day_prev_month.strftime("%Y-%m-%d")
            )

            if data:
                rows = self.prepare_rows(data, self.get_existing_timestamps())
                self.upload_to_sheets(rows)
                state["first_run"] = False
                state["last_run"] = datetime.now().isoformat()
                self.save_state(state)
                success = True
        else:
            # Incremental next 12 hours
            data = self.fetch_weather_data()
            if data:
                rows = self.prepare_rows(data, self.get_existing_timestamps())
                self.upload_to_sheets(rows)
                state["last_run"] = datetime.now().isoformat()
                self.save_state(state)
                success = True

        if success:
            logger.info("🎉 ETL process completed successfully!")
        else:
            logger.error("❌ ETL process failed")
        return success

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    etl = WeatherETL(
        service_account_file=os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json"),
        sheet_name=os.getenv("SHEET_NAME", "Weather_Data"),
        lat=float(os.getenv("LATITUDE", 28.61)),
        lon=float(os.getenv("LONGITUDE", 77.23))
    )
    success = etl.run()
    if not success:
        exit(1)
