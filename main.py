import requests
import sqlite3
import json
import time
import matplotlib.pyplot as plt
import numpy as np


# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

# API Keys
OPENWEATHER_API_KEY = "d2b6933ae0a235dbdb6209f0ac09cb8e"
OPENUV_API_KEY = "openuv-ac490wrminnpcmb-io"
WEATHERAPI_KEY = "0e6637b961c84282bb704311250112"

# API Base URLs
OPENWEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
OPENUV_BASE_URL = "https://api.openuv.io/api/v1/uv"
WEATHERAPI_BASE_URL = "http://api.weatherapi.com/v1/current.json"

# Database
DB_NAME = "weather_data.db"
OUTPUT_FILE = "calculations_output.txt"

# Cities list (25 cities)
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
    "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "Indianapolis",
    "San Francisco", "Seattle", "Denver", "Boston", "Nashville",
    "Detroit", "Portland", "Las Vegas", "Memphis", "Louisville"
]

# City coordinates for UV API (latitude, longitude)
CITY_COORDS = {
    "New York": (40.7128, -74.0060),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago": (41.8781, -87.6298),
    "Houston": (29.7604, -95.3698),
    "Phoenix": (33.4484, -112.0740),
    "Philadelphia": (39.9526, -75.1652),
    "San Antonio": (29.4241, -98.4936),
    "San Diego": (32.7157, -117.1611),
    "Dallas": (32.7767, -96.7970),
    "Austin": (30.2672, -97.7431),
    "Jacksonville": (30.3322, -81.6557),
    "Fort Worth": (32.7555, -97.3308),
    "Columbus": (39.9612, -82.9988),
    "Charlotte": (35.2271, -80.8431),
    "Indianapolis": (39.7684, -86.1581),
    "San Francisco": (37.7749, -122.4194),
    "Seattle": (47.6062, -122.3321),
    "Denver": (39.7392, -104.9903),
    "Boston": (42.3601, -71.0589),
    "Nashville": (36.1627, -86.7816),
    "Detroit": (42.3314, -83.0458),
    "Portland": (45.5152, -122.6784),
    "Las Vegas": (36.1699, -115.1398),
    "Memphis": (35.1495, -90.0490),
    "Louisville": (38.2527, -85.7585)
}


# ============================================================================
# DATABASE SETUP FUNCTIONS
# ============================================================================

def init_database():
    """Initializes the database and creates all necessary tables."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Cities table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Cities (
            city_id INTEGER PRIMARY KEY,
            city_name TEXT UNIQUE
        )
    ''')
    
    # Weather Data table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Weather_Data (
            id INTEGER PRIMARY KEY,
            city_id INTEGER,
            temperature REAL,
            weather_condition TEXT,
            timestamp TEXT,
            FOREIGN KEY (city_id) REFERENCES Cities(city_id)
        )
    ''')
    
    # UV Data table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS UV_Data (
            id INTEGER PRIMARY KEY,
            city_id INTEGER,
            uv_index REAL,
            timestamp TEXT,
            FOREIGN KEY (city_id) REFERENCES Cities(city_id)å
        )
    ''')
    
    # Air Quality Data table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Air_Quality_Data (
            id INTEGER PRIMARY KEY,
            city_id INTEGER,
            aqi_value REAL,
            timestamp TEXT,
            FOREIGN KEY (city_id) REFERENCES Cities(city_id)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully!")


# ============================================================================
# DATA COLLECTION FUNCTIONS - ELLA (Weather)
# ============================================================================

def store_weather(city_names, api_key, db_name=DB_NAME):
    """Fetches weather data from OpenWeatherMap API and stores it in database."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    store_count = 0
    max_stores = 25
    
    for city in city_names:
        if store_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
            # Check if data already exists for today
            cur.execute('''
                SELECT COUNT(*) FROM Weather_Data
                JOIN Cities ON Weather_Data.city_id = Cities.city_id
                WHERE Cities.city_name = ? AND DATE(timestamp) = DATE('now')
            ''', (city,))
            
            if cur.fetchone()[0] > 0:
                print(f"Weather data for {city} already exists for today, skipping...")
                continue
            
            # Make API request
            params = {
                'q': city,
                'appid': api_key,
                'units': 'imperial'
            }
            
            response = requests.get(OPENWEATHER_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Extract weather data
            temperature = data['main']['temp']
            weather_condition = data['weather'][0]['main']
            
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Get or create city_id
            cur.execute('SELECT city_id FROM Cities WHERE city_name = ?', (city,))
            result = cur.fetchone()
            
            if result:
                city_id = result[0]
            else:
                cur.execute('SELECT MAX(city_id) FROM Cities')
                max_id = cur.fetchone()[0]
                city_id = 1 if max_id is None else max_id + 1
                cur.execute('INSERT INTO Cities (city_id, city_name) VALUES (?, ?)', 
                           (city_id, city))
            
            # Get next id for Weather_Data
            cur.execute('SELECT MAX(id) FROM Weather_Data')
            max_id = cur.fetchone()[0]
            weather_id = 1 if max_id is None else max_id + 1
            
            # Insert weather data
            cur.execute('''
                INSERT INTO Weather_Data (id, city_id, temperature, weather_condition, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (weather_id, city_id, temperature, weather_condition, timestamp))
            
            conn.commit()
            store_count += 1
            print(f'Stored weather data for {city}: Temp = {temperature}°F, Condition = {weather_condition}')
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error for {city}: {e}")
            continue
    
    conn.close()
    print(f"\nTotal weather records stored: {store_count}")
    return store_count


# ============================================================================
# DATA COLLECTION FUNCTIONS - EMMA (UV)
# ============================================================================

def store_uv(city_names, api_key, city_coordinates, db_name=DB_NAME):
    """Fetches UV data from OpenUV API and stores it in database."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    stored_count = 0
    max_stores = 25
    
    for city in city_names:
        if stored_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
            # Check if data already exists for today
            cur.execute('''
                SELECT COUNT(*) FROM UV_Data
                JOIN Cities ON UV_Data.city_id = Cities.city_id
                WHERE Cities.city_name = ? AND DATE(timestamp) = DATE('now')
            ''', (city,))
            
            if cur.fetchone()[0] > 0:
                print(f"UV data for {city} already exists for today, skipping...")
                continue
            
            if city not in city_coordinates:
                print(f"Coordinates not found for {city}, skipping...")
                continue
            
            lat, lon = city_coordinates[city]
            
            headers = {'x-access-token': api_key}
            params = {'lat': lat, 'lng': lon}
            
            response = requests.get(OPENUV_BASE_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            uv_index = data['result']['uv']
            timestamp = data['result']['uv_time']
            
            # Get or create city_id
            cur.execute('SELECT city_id FROM Cities WHERE city_name = ?', (city,))
            result = cur.fetchone()
            
            if result:
                city_id = result[0]
            else:
                cur.execute('SELECT MAX(city_id) FROM Cities')
                max_id = cur.fetchone()[0]
                city_id = 1 if max_id is None else max_id + 1
                cur.execute('INSERT INTO Cities (city_id, city_name) VALUES (?, ?)', 
                           (city_id, city))
            
            # Get next id for UV_Data
            cur.execute('SELECT MAX(id) FROM UV_Data')
            max_id = cur.fetchone()[0]
            uv_id = 1 if max_id is None else max_id + 1
            
            # Insert UV data
            cur.execute('''
                INSERT INTO UV_Data (id, city_id, uv_index, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (uv_id, city_id, uv_index, timestamp))
            
            conn.commit()
            stored_count += 1
            print(f'Stored UV data for {city}: UV Index = {uv_index}')
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error for {city}: {e}")
            continue
    
    conn.close()
    print(f"\nTotal UV records stored: {stored_count}")
    return stored_count


# ============================================================================
# DATA COLLECTION FUNCTIONS - MINDY (Air Quality)
# ============================================================================

def store_air_quality(city_names, api_key, db_name=DB_NAME):
    """Fetches air quality data from WeatherAPI and stores it in database."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    store_count = 0
    max_stores = 25
    
    for city in city_names:
        if store_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
            # Check if data already exists for today
            cur.execute('''
                SELECT COUNT(*) FROM Air_Quality_Data
                JOIN Cities ON Air_Quality_Data.city_id = Cities.city_id
                WHERE Cities.city_name = ? AND DATE(timestamp) = DATE('now')
            ''', (city,))
            
            if cur.fetchone()[0] > 0:
                print(f"Air quality data for {city} already exists for today, skipping...")
                continue
            
            # Make API request
            params = {
                'key': api_key,
                'q': city,
                'aqi': 'yes'
            }
            
            response = requests.get(WEATHERAPI_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            aqi_value = data['current']['air_quality']['us-epa-index']
            timestamp = data['current']['last_updated']
            
            # Get or create city_id
            cur.execute('SELECT city_id FROM Cities WHERE city_name = ?', (city,))
            result = cur.fetchone()
            
            if result:
                city_id = result[0]
            else:
                cur.execute('SELECT MAX(city_id) FROM Cities')
                max_id = cur.fetchone()[0]
                city_id = 1 if max_id is None else max_id + 1
                cur.execute('INSERT INTO Cities (city_id, city_name) VALUES (?, ?)', 
                           (city_id, city))
            
            # Get next id for Air_Quality_Data
            cur.execute('SELECT MAX(id) FROM Air_Quality_Data')
            max_id = cur.fetchone()[0]
            aqi_id = 1 if max_id is None else max_id + 1
            
            # Insert air quality data
            cur.execute('''
                INSERT INTO Air_Quality_Data (id, city_id, aqi_value, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (aqi_id, city_id, aqi_value, timestamp))
            
            conn.commit()
            store_count += 1
            print(f'Stored air quality data for {city}: AQI = {aqi_value}')
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error for {city}: {e}")
            continue
    
    conn.close()
    print(f"\nTotal air quality records stored: {store_count}")
    return store_count


# ============================================================================
# CALCULATION FUNCTIONS
# ============================================================================

def calculate_avg_temp(db_conn, city_id=None):
    """Calculates average temperature from weather data."""
    cur = db_conn.cursor()
    
    with open(OUTPUT_FILE, 'a') as f:
        f.write("\n" + "="*50 + "\n")
        f.write("AVERAGE TEMPERATURE CALCULATIONS\n")
        f.write("="*50 + "\n\n")
        
        if city_id is None:
            cur.execute('''
                SELECT Cities.city_name, AVG(Weather_Data.temperature) as avg_temp
                FROM Weather_Data
                JOIN Cities ON Weather_Data.city_id = Cities.city_id
                GROUP BY Cities.city_id, Cities.city_name
                ORDER BY avg_temp DESC
            ''')
            
            results = cur.fetchall()
            
            if not results:
                print("No weather data found")
                f.write("No weather data found\n")
                return None
            
            cur.execute('SELECT AVG(temperature) FROM Weather_Data')
            overall_avg = cur.fetchone()[0]
            
            f.write(f"Overall Average Temperature: {overall_avg:.2f}°F\n\n")
            f.write("Average Temperature by City:\n")
            f.write("-" * 40 + "\n")
            
            for city_name, avg_temp in results:
                f.write(f"{city_name:25s}: {avg_temp:.2f}°F\n")
            
            return overall_avg
        else:
            cur.execute('''
                SELECT Cities.city_name, AVG(Weather_Data.temperature) as avg_temp
                FROM Weather_Data
                JOIN Cities ON Weather_Data.city_id = Cities.city_id
                WHERE Cities.city_id = ?
                GROUP BY Cities.city_id, Cities.city_name
            ''', (city_id,))
            
            result = cur.fetchone()
            if result:
                city_name, avg_temp = result
                f.write(f"City: {city_name}\n")
                f.write(f"Average Temperature: {avg_temp:.2f}°F\n")
                return avg_temp
            return None


def calculate_avg_uv(db_conn, city_id=None):
    """Calculates average UV index from UV data."""
    cur = db_conn.cursor()
    
    if city_id is None:
        cur.execute('''
            SELECT Cities.city_name, AVG(UV_Data.uv_index) as avg_uv
            FROM UV_Data
            JOIN Cities ON UV_Data.city_id = Cities.city_id
            GROUP BY Cities.city_id, Cities.city_name
            ORDER BY avg_uv
        ''')
        results = cur.fetchall()
        
        with open(OUTPUT_FILE, 'a') as f:
            f.write("\n" + "="*50 + "\n")
            f.write("AVERAGE UV INDEX BY CITY\n")
            f.write("="*50 + "\n")
            for city_name, avg_uv in results:
                f.write(f"{city_name}: {avg_uv:.2f}\n")
        
        cur.execute('SELECT AVG(uv_index) FROM UV_Data')
        overall_avg = cur.fetchone()[0]
        return overall_avg if overall_avg else 0.0
    else:
        cur.execute('''
            SELECT AVG(uv_index)
            FROM UV_Data
            WHERE city_id = ?
        ''', (city_id,))
        avg_uv = cur.fetchone()[0]
        return avg_uv if avg_uv else 0.0


def calculate_avg_aqi(db_conn, city_id=None):
    """Calculates average Air Quality Index from air quality data."""
    cur = db_conn.cursor()
    
    with open(OUTPUT_FILE, 'a') as f:
        f.write("\n" + "="*50 + "\n")
        f.write("AVERAGE AQI CALCULATIONS\n")
        f.write("="*50 + "\n\n")
        
        if city_id is None:
            cur.execute('''
                SELECT Cities.city_name, AVG(Air_Quality_Data.aqi_value) as avg_aqi
                FROM Air_Quality_Data
                JOIN Cities ON Air_Quality_Data.city_id = Cities.city_id
                GROUP BY Cities.city_id, Cities.city_name
                ORDER BY avg_aqi
            ''')
            results = cur.fetchall()
            
            if not results:
                print('No air quality data found')
                f.write('No air quality data found\n')
                return None
            
            cur.execute('SELECT AVG(aqi_value) FROM Air_Quality_Data')
            overall_avg = cur.fetchone()[0]
            f.write(f"Overall Average AQI: {overall_avg:.2f}\n\n")
            f.write("Average AQI by City:\n")
            f.write("-" * 40 + "\n")
            
            for city_name, avg_aqi in results:
                f.write(f"{city_name:25s}: {avg_aqi:.2f}\n")
            
            return overall_avg
        else:
            cur.execute('''
                SELECT Cities.city_name, AVG(Air_Quality_Data.aqi_value) as avg_aqi
                FROM Air_Quality_Data
                JOIN Cities ON Air_Quality_Data.city_id = Cities.city_id
                WHERE Cities.city_id = ?
                GROUP BY Cities.city_id, Cities.city_name
            ''', (city_id,))
            
            result = cur.fetchone()
            if result:
                city_name, avg_aqi = result
                f.write(f"City: {city_name}\n")
                f.write(f"Average AQI: {avg_aqi:.2f}\n")
                return avg_aqi
            return None


# ============================================================================
# CALCULATION FUNCTIONS - EVERYONE
# ============================================================================

def calculate_safety_score(db_conn):
    """
    Calculates composite outdoor activity safety score for each city.
    
    Input: 
        - db_conn: Database connection object (sqlite3.Connection)
    
    Output: 
        - Dictionary mapping city names to safety scores (dict)
        - Writes ranked list to calculations_output.txt
    
    Process:
    - Performs JOIN operations across Weather_Data, UV_Data, and Air_Quality_Data tables
    - Links all tables through city_id from Cities table
    - Calculates averages for each city
    - Applies composite formula:
        * Lower UV index = safer (less sun exposure)
        * Lower AQI = safer (cleaner air)
        * Temperature near 70°F = more comfortable
    - Ranks cities from safest to least safe
    - Returns dictionary and writes formatted rankings to file
    """
    pass


# ============================================================================
# VISUALIZATION HELPER FUNCTION
# ============================================================================

def get_calculated_data(db_conn):
    """
    Retrieves all calculated data needed for visualizations.
    
    Input: 
        - db_conn: Database connection object (sqlite3.Connection)
    
    Output: 
        - Dictionary containing all visualization data (dict)
    
    Returns structure:
    {
        'cities': [list of city names],
        'safety_scores': [list of scores],
        'avg_temps': [list of temperatures],
        'avg_uv': [list of UV indices],
        'avg_aqi': [list of AQI values]
    }
    
    Process:
    - Performs JOIN across all tables
    - Groups data by city
    - Calculates averages
    - Formats data for easy plotting
    """
    pass


# ============================================================================
# VISUALIZATION FUNCTIONS - EVERYONE
# ============================================================================

def create_safety_ranking_chart(calculated_data):
    """
    Creates ranked bar chart of top 10 safest cities.
    
    Input: 
        - calculated_data: Dictionary with city data (dict)
    
    Output: 
        - Saves 'safety_ranking.png' image file
    
    Visualization:
    - Bar chart showing top 10 cities
    - X-axis: City names
    - Y-axis: Safety scores (lower = safer)
    - Custom colors to differentiate bars
    - Title and labels
    """
    pass


def create_grouped_comparison_chart(calculated_data):
    """
    Creates grouped bar chart comparing temperature, UV, and AQI.
    
    Input: 
        - calculated_data: Dictionary with city data (dict)
    
    Output: 
        - Saves 'grouped_comparison.png' image file
    
    Visualization:
    - Grouped bars for selected cities
    - Three bars per city (temp, UV, AQI)
    - Different colors for each metric
    - Legend to identify metrics
    - Shows which factors contribute to each city's score
    """
    pass


def create_scatter_plot(calculated_data):
    """
    Creates scatter plot of temperature vs AQI with UV as color.
    
    Input: 
        - calculated_data: Dictionary with city data (dict)
    
    Output: 
        - Saves 'scatter_temp_aqi.png' image file
    
    Visualization:
    - X-axis: Temperature
    - Y-axis: AQI
    - Point color: UV index intensity (colormap)
    - Color bar showing UV scale
    - Reveals correlations between metrics
    """
    pass


def create_horizontal_rankings(calculated_data):
    """
    Creates three separate horizontal bar charts for individual rankings.
    
    Input: 
        - calculated_data: Dictionary with city data (dict)
    
    Output: 
        - Saves three image files:
            * 'ranking_temperature.png'
            * 'ranking_uv.png'
            * 'ranking_aqi.png'
    
    Visualizations:
    - Three separate charts
    - Each ranks cities by one metric
    - Horizontal bars for easier city name reading
    - Shows which cities excel in specific categories
    """
    pass


def create_heatmap(calculated_data):
    """
    Creates color-coded heatmap grid of all metrics.
    
    Input: 
        - calculated_data: Dictionary with city data (dict)
    
    Output: 
        - Saves 'heatmap_all_metrics.png' image file
    
    Visualization:
    - Rows: Cities
    - Columns: Temp, UV, AQI, Safety Score
    - Color coding: Green (better) to Red (worse)
    - Comprehensive at-a-glance comparison
    - Normalized values for comparison
    """
    pass


def create_visualizations(calculated_data):
    """
    Master function that generates all visualizations.
    
    Input: 
        - calculated_data: Dictionary with city data (dict)
    
    Output: 
        - All visualization image files saved
    
    Process:
    - Calls all individual visualization functions
    - Ensures consistent styling across charts
    - Saves all files with descriptive names
    """
    pass


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function that coordinates the entire program.
    
    Process:
    1. Initialize database
    2. Collect data from APIs (if needed)
    3. Perform calculations
    4. Create visualizations
    """
    print("Initializing database...")
    init_database()
    
    # Collect weather data (Ella's part)
    print("\n" + "="*50)
    print("COLLECTING WEATHER DATA")
    print("="*50)
    store_weather(CITIES, OPENWEATHER_API_KEY)
    
    # Collect UV data (Emma's part)
    print("\n" + "="*50)
    print("COLLECTING UV DATA")
    print("="*50)
    store_uv(CITIES, OPENUV_API_KEY, CITY_COORDS)
    
    # Collect air quality data (Mindy's part)
    print("\n" + "="*50)
    print("COLLECTING AIR QUALITY DATA")
    print("="*50)
    store_air_quality(CITIES, WEATHERAPI_KEY)
    
    # Perform calculations
    print("\n" + "="*50)
    print("PERFORMING CALCULATIONS")
    print("="*50)
    
    # Clear the output file
    with open(OUTPUT_FILE, 'w') as f:
        f.write("WEATHER DATA ANALYSIS RESULTS\n")
        f.write("="*50 + "\n")
    
    conn = sqlite3.connect(DB_NAME)
    
    # Calculate averages
    avg_temp = calculate_avg_temp(conn)
    avg_uv = calculate_avg_uv(conn)
    avg_aqi = calculate_avg_aqi(conn)
    
    print(f"\nOverall Average Temperature: {avg_temp:.2f}°F" if avg_temp else "No temperature data")
    print(f"Overall Average UV Index: {avg_uv:.2f}" if avg_uv else "No UV data")
    print(f"Overall Average AQI: {avg_aqi:.2f}" if avg_aqi else "No AQI data")
    
    conn.close()
    
    print("\n" + "="*50)
    print("COMPLETE! Check calculations_output.txt for detailed results.")
    print("="*50)
