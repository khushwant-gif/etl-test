import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import logging
import time
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
        
    def load_state(self):
        """Load the last run state from file"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    logger.info(f"📁 Loaded state: {state}")
                    return state
        except Exception as e:
            logger.warning(f"⚠️ Error loading state file: {e}")
        
        # Default state for first run
        return {"last_run": None, "first_run": True}
    
    def save_state(self, state):
        """Save the current run state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
            logger.info(f"💾 Saved state: {state}")
        except Exception as e:
            logger.error(f"❌ Error saving state: {e}")
    
    def get_existing_timestamps(self):
        """Get existing timestamps from Google Sheets to avoid duplicates"""
        try:
            all_values = self.sheet.get_all_values()
            if not all_values:
                return set()
            
            timestamps = set()
            for row in all_values[1:]:
                if row and row[0]:
                    timestamps.add(row[0])
            
            logger.info(f"📋 Found {len(timestamps)} existing timestamps")
            return timestamps
        except Exception as e:
            logger.error(f"❌ Error getting existing timestamps: {e}")
            return set()
        
    def setup_google_sheets(self):
        """Initialize Google Sheets connection"""
        try:
            if not os.path.exists(self.SERVICE_ACCOUNT_FILE):
                raise FileNotFoundError(f"Service account file not found: {self.SERVICE_ACCOUNT_FILE}")
                
            scope = ["https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.SERVICE_ACCOUNT_FILE, scope)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open(self.SHEET_NAME).sheet1
            logger.info("✅ Connected to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"❌ Error connecting to Google Sheets: {e}")
            return False
    
    def fetch_weather_data(self, start_date=None, end_date=None, forecast_days=1):
        """Fetch weather data from Open-Meteo API"""
        params = {
            "latitude": self.LAT,
            "longitude": self.LON,
            "hourly": ["temperature_2m", "relative_humidity_2m", "visibility", "weathercode", "precipitation"],
            "timezone": "Asia/Kolkata"
        }
        
        if start_date and end_date:
            API_URL = "https://archive-api.open-meteo.com/v1/archive"
            params.update({"start_date": start_date, "end_date": end_date})
            logger.info(f"📅 Fetching historical data from {start_date} to {end_date}")
        else:
            API_URL = "https://api.open-meteo.com/v1/forecast"
            params["forecast_days"] = forecast_days
            logger.info(f"🔮 Fetching forecast data for {forecast_days} days")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(API_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                if "hourly" not in data or not data["hourly"].get("time"):
                    raise ValueError("Invalid API response structure")
                logger.info("🌍 Weather data fetched successfully")
                return data
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error("❌ Failed to fetch weather data after retries")
                    return None
            except ValueError as e:
                logger.error(f"❌ Data validation error: {e}")
                return None
    
    def prepare_data(self, data, existing_timestamps=None):
        """Transform weather data into list of rows, filtering duplicates"""
        try:
            hourly = data["hourly"]
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            humid = hourly.get("relative_humidity_2m", [])
            vis = hourly.get("visibility", [])
            weather = hourly.get("weathercode", [])
            precip = hourly.get("precipitation", [])
            
            if not times:
                raise ValueError("No time data received from API")
            
            rows = []
            fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            existing_timestamps = existing_timestamps or set()
            
            for i in range(len(times)):
                timestamp = times[i]
                if timestamp in existing_timestamps:
                    continue
                
                row = [
                    timestamp,
                    temps[i] if i < len(temps) else None,
                    humid[i] if i < len(humid) else None,
                    vis[i] if i < len(vis) else 24000,
                    weather[i] if i < len(weather) else 0,
                    precip[i] if i < len(precip) else 0,
                    fetched_at
                ]
                rows.append(row)
            
            logger.info(f"📊 Prepared {len(rows)} new rows")
            return rows
        except Exception as e:
            logger.error(f"❌ Error preparing data: {e}")
            return None
    
    def upload_to_sheets(self, rows):
        """Upload data rows to Google Sheets"""
        try:
            if not rows:
                logger.info("📋 No new data to upload")
                return True
            
            headers = ["Time","Temperature_2m","Humidity_2m","Visibility","WeatherCode","Precipitation","Fetched_At"]
            if self.sheet.row_count == 0 or len(self.sheet.row_values(1)) == 0:
                self.sheet.append_row(headers)
                logger.info("📋 Added headers to sheet")
            
            batch_size = 100
            total_uploaded = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                self.sheet.append_rows(batch)
                total_uploaded += len(batch)
                if i + batch_size < len(rows):
                    time.sleep(1)
            
            logger.info(f"✅ Uploaded {total_uploaded} rows to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"❌ Error uploading to Google Sheets: {e}")
            return False
    
    def run_initial_load(self):
        """Fetch previous month's data on first run"""
        logger.info("🔄 Running initial load - fetching previous month's data")
        today = datetime.now().date()
        first_day_current_month = today.replace(day=1)
        last_day_prev_month = first_day_current_month - timedelta(days=1)
        first_day_prev_month = last_day_prev_month.replace(day=1)
        
        weather_data = self.fetch_weather_data(
            start_date=first_day_prev_month.strftime("%Y-%m-%d"),
            end_date=last_day_prev_month.strftime("%Y-%m-%d")
        )
        if not weather_data:
            logger.error("❌ Failed to fetch historical data")
            return False
        
        rows = self.prepare_data(weather_data, self.get_existing_timestamps())
        return self.upload_to_sheets(rows)
    
    def run_incremental_update(self):
        """Fetch next 12 hours forecast"""
        logger.info("🔄 Running incremental update")
        weather_data = self.fetch_weather_data(forecast_days=1)
        if not weather_data:
            logger.error("❌ Failed to fetch incremental data")
            return False
        rows = self.prepare_data(weather_data, self.get_existing_timestamps())
        return self.upload_to_sheets(rows)
    
    def run(self):
        logger.info("🚀 Starting Weather ETL process")
        state = self.load_state()
        
        if not self.setup_google_sheets():
            return False
        
        success = False
        if state.get("first_run", True):
            success = self.run_initial_load()
            if success:
                state = {"last_run": datetime.now().isoformat(), "first_run": False}
                self.save_state(state)
        else:
            success = self.run_incremental_update()
            if success:
                state["last_run"] = datetime.now().isoformat()
                self.save_state(state)
        
        if success:
            logger.info("🎉 ETL process completed successfully!")
        else:
            logger.error("❌ ETL process failed")
        return success

def main():
    config = {
        'service_account_file': os.getenv('SERVICE_ACCOUNT_FILE', 'service_account.json'),
        'sheet_name': os.getenv('SHEET_NAME', 'Weather_Data'),
        'lat': float(os.getenv('LATITUDE', '28.61')),
        'lon': float(os.getenv('LONGITUDE', '77.23'))
    }
    logger.info(f"🌍 Coordinates: {config['lat']}, {config['lon']}")
    etl = WeatherETL(**config)
    success = etl.run()
    if not success:
        exit(1)

if __name__ == "__main__":
    main()
