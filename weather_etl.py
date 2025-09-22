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
        return {
            "last_run": None,
            "first_run": True
        }
    
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
            # Get all values from the sheet
            all_values = self.sheet.get_all_values()
            
            if not all_values:
                return set()
            
            # Find the Time column (assuming it's the first column)
            timestamps = set()
            for row in all_values[1:]:  # Skip header row
                if row and row[0]:  # Check if time column has data
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
            "hourly": ["temperature_2m", "relative_humidity_2m", 
                      "visibility", "weathercode", "precipitation"],
            "timezone": "Asia/Kolkata"
        }
        
        # For historical data (past week on first run)
        if start_date and end_date:
            # Use historical weather API for past data
            API_URL = "https://archive-api.open-meteo.com/v1/archive"
            params.update({
                "start_date": start_date,
                "end_date": end_date
            })
            logger.info(f"📅 Fetching historical data from {start_date} to {end_date}")
        else:
            # For current/forecast data
            API_URL = "https://api.open-meteo.com/v1/forecast"
            params["forecast_days"] = forecast_days
            logger.info(f"🔮 Fetching forecast data for {forecast_days} days")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(API_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # Validate response has required data
                if "hourly" not in data or not data["hourly"].get("time"):
                    logger.warning(f"⚠️ Invalid response structure: {data}")
                    raise ValueError("Invalid API response structure")
                
                logger.info("🌍 Weather data fetched successfully")
                logger.debug(f"📊 Received {len(data['hourly']['time'])} hourly records")
                return data
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"❌ Failed to fetch weather data after {max_retries} attempts")
                    return None
            except ValueError as e:
                logger.error(f"❌ Data validation error: {e}")
                return None
    
    def prepare_data(self, data, existing_timestamps=None):
        """Transform weather data into list of rows, filtering duplicates and incomplete data"""
        try:
            hourly = data["hourly"]
            
            # Get data arrays
            times = hourly.get("time", [])
            temperatures = hourly.get("temperature_2m", [])
            humidity = hourly.get("relative_humidity_2m", [])
            visibility = hourly.get("visibility", [])
            weather_codes = hourly.get("weathercode", [])
            precipitation = hourly.get("precipitation", [])
            
            # Check if we have data
            if not times:
                raise ValueError("No time data received from API")
            
            # Prepare rows
            rows = []
            fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            existing_timestamps = existing_timestamps or set()
            
            # Use times length as primary reference
            duplicate_count = 0
            incomplete_count = 0
            
            for i in range(len(times)):
                timestamp = times[i] if i < len(times) else ''
                
                # Skip if this timestamp already exists
                if timestamp in existing_timestamps:
                    duplicate_count += 1
                    continue
                
                # Get values with safe indexing
                temp = temperatures[i] if i < len(temperatures) else None
                humid = humidity[i] if i < len(humidity) else None
                vis = visibility[i] if i < len(visibility) else None
                weather = weather_codes[i] if i < len(weather_codes) else None
                precip = precipitation[i] if i < len(precipitation) else None
                
                # Skip rows with critical missing data (temperature and humidity)
                if temp is None or humid is None:
                    incomplete_count += 1
                    logger.warning(f"⚠️ Skipping incomplete data for {timestamp}")
                    continue
                
                # Handle missing optional fields with defaults
                vis = vis if vis is not None else 24000  # Default visibility in meters
                weather = weather if weather is not None else 0  # Clear sky default
                precip = precip if precip is not None else 0  # No precipitation default
                
                row = [
                    timestamp,
                    temp,
                    humid,
                    vis,
                    weather,
                    precip,
                    fetched_at
                ]
                rows.append(row)
            
            logger.info(f"📊 Prepared {len(rows)} new rows ({duplicate_count} duplicates, {incomplete_count} incomplete records skipped)")
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
            
            # Define headers
            headers = [
                "Time", "Temperature_2m", "Humidity_2m", 
                "Visibility", "WeatherCode", "Precipitation", "Fetched_At"
            ]
            
            # Check if sheet is empty and add headers
            if self.sheet.row_count == 0 or len(self.sheet.row_values(1)) == 0:
                self.sheet.append_row(headers)
                logger.info("📋 Added headers to sheet")
            
            # Upload data in batches for better performance
            batch_size = 100
            total_uploaded = 0
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                self.sheet.append_rows(batch)
                total_uploaded += len(batch)
                logger.info(f"📤 Uploaded batch: {len(batch)} rows")
                
                # Small delay to avoid rate limits
                if i + batch_size < len(rows):
                    time.sleep(1)
            
            logger.info(f"✅ Successfully uploaded {total_uploaded} rows to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"❌ Error uploading to Google Sheets: {e}")
            return False
    
    def run_initial_load(self):
        """Run initial load with past week's data"""
        logger.info("🔄 Running initial load - fetching past week's data...")
        
        # Calculate date range for past week (excluding today to avoid incomplete data)
        today = datetime.now().date()
        end_date = today - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=6)  # 7 days total
        
        logger.info(f"📅 Loading data from {start_date} to {end_date}")
        
        # Fetch historical data
        weather_data = self.fetch_weather_data(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )
        
        if not weather_data:
            logger.error("❌ Failed to fetch initial historical data")
            return False
        
        # Get existing timestamps to avoid duplicates
        existing_timestamps = self.get_existing_timestamps()
        
        # Prepare data
        rows = self.prepare_data(weather_data, existing_timestamps)
        if not rows:
            logger.info("📋 No new historical data to upload")
            return True
        
        # Upload to sheets
        return self.upload_to_sheets(rows)
    
    def run_incremental_update(self):
        """Run incremental update with recent data"""
        logger.info("🔄 Running incremental update...")
        
        # Fetch current forecast data (next 1-2 days)
        weather_data = self.fetch_weather_data(forecast_days=2)
        
        if not weather_data:
            logger.error("❌ Failed to fetch incremental data")
            return False
        
        # Get existing timestamps to avoid duplicates
        existing_timestamps = self.get_existing_timestamps()
        
        # Prepare data
        rows = self.prepare_data(weather_data, existing_timestamps)
        
        # Upload to sheets
        return self.upload_to_sheets(rows)
    
    def run(self):
        """Execute the complete ETL process"""
        logger.info("🚀 Starting Weather ETL process...")
        
        # Load previous state
        state = self.load_state()
        
        # Setup Google Sheets connection
        if not self.setup_google_sheets():
            return False
        
        # Determine run type
        success = False
        if state.get("first_run", True):
            # First run - load past week's data
            success = self.run_initial_load()
            if success:
                # Update state
                state = {
                    "last_run": datetime.now().isoformat(),
                    "first_run": False
                }
                self.save_state(state)
        else:
            # Subsequent runs - incremental updates
            success = self.run_incremental_update()
            if success:
                # Update last run time
                state["last_run"] = datetime.now().isoformat()
                self.save_state(state)
        
        if success:
            logger.info("🎉 ETL process completed successfully!")
        else:
            logger.error("❌ ETL process failed")
            
        return success

def main():
    """Main execution function"""
    # Configuration
    config = {
        'service_account_file': os.getenv('SERVICE_ACCOUNT_FILE', 'service_account.json'),
        'sheet_name': os.getenv('SHEET_NAME', 'Weather_Data'),
        'lat': float(os.getenv('LATITUDE', '28.61')),  # Delhi latitude
        'lon': float(os.getenv('LONGITUDE', '77.23'))   # Delhi longitude
    }
    
    logger.info(f"🌍 Coordinates: {config['lat']}, {config['lon']}")
    logger.info(f"📊 Target sheet: {config['sheet_name']}")
    
    # Initialize and run ETL
    etl = WeatherETL(**config)
    success = etl.run()
    
    if not success:
        logger.error("❌ ETL process failed")
        exit(1)

if __name__ == "__main__":
    main()
