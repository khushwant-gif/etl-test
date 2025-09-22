import os
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import logging
import time

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
    
    def fetch_weather_data(self):
        """Fetch weather data from Open-Meteo API"""
        API_URL = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": self.LAT,
            "longitude": self.LON,
            "hourly": ["temperature_2m", "relative_humidity_2m", 
                      "visibility", "weathercode", "precipitation"],
            "timezone": "Asia/Kolkata",
            "forecast_days": 1  # Limit to today's data
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(API_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.info("🌍 Weather data fetched successfully")
                return data
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"❌ Failed to fetch weather data after {max_retries} attempts")
                    return None
    
    def prepare_data(self, data):
        """Transform weather data into list of rows"""
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
            
            # Get the minimum length to avoid index errors
            min_length = min(len(arr) for arr in [times, temperatures, humidity, 
                           visibility, weather_codes, precipitation] if arr)
            
            for i in range(min_length):
                row = [
                    times[i] if i < len(times) else '',
                    temperatures[i] if i < len(temperatures) else '',
                    humidity[i] if i < len(humidity) else '',
                    visibility[i] if i < len(visibility) else '',
                    weather_codes[i] if i < len(weather_codes) else '',
                    precipitation[i] if i < len(precipitation) else '',
                    fetched_at
                ]
                rows.append(row)
            
            logger.info(f"📊 Prepared {len(rows)} rows of data")
            return rows
        except Exception as e:
            logger.error(f"❌ Error preparing data: {e}")
            return None
    
    def upload_to_sheets(self, rows):
        """Upload data rows to Google Sheets"""
        try:
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
            
            logger.info(f"✅ Successfully uploaded {total_uploaded} rows to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"❌ Error uploading to Google Sheets: {e}")
            return False
    
    def run(self):
        """Execute the complete ETL process"""
        logger.info("🚀 Starting Weather ETL process...")
        
        # Setup Google Sheets connection
        if not self.setup_google_sheets():
            return False
        
        # Fetch weather data
        weather_data = self.fetch_weather_data()
        if not weather_data:
            return False
        
        # Prepare data rows
        rows = self.prepare_data(weather_data)
        if not rows:
            logger.error("❌ No data to upload")
            return False
        
        # Upload to Google Sheets
        if not self.upload_to_sheets(rows):
            return False
        
        logger.info("🎉 ETL process completed successfully!")
        return True

def main():
    """Main execution function"""
    # Configuration - can be moved to config file or environment variables
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
