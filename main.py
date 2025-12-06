import requests
import sqlite3
import json
import time
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# ============================================================================
# API KEY LOADER FUNCTION
# ============================================================================

def get_api_key(filename):
    '''
    loads in API key from file 

    ARGUMENTS:  
        filename: file that contains your API key
    
    RETURNS:
        your API key
    '''
    try:
        with open(filename, 'r') as f:
            return f.read().strip()
    except:
        return None

# ============================================================================
# CONSTANTS AND CONFIGURATION
# ============================================================================

# Load API keys from text files
OPENWEATHER_API_KEY = get_api_key('openweather_api_key.txt')
OPENUV_API_KEY = get_api_key('openuv_api_key.txt')
WEATHERAPI_KEY = get_api_key('weatherapi_api_key.txt')

# API URLs
OPENWEATHER_BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'
OPENUV_BASE_URL = 'https://api.openuv.io/api/v1/uv'
WEATHERAPI_BASE_URL = 'http://api.weatherapi.com/v1/current.json'

# Database settings
DB_NAME = 'weather_data.db'
OUTPUT_FILE = 'calculations_output.txt'

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
            FOREIGN KEY (city_id) REFERENCES Cities(city_id)
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
# CALCULATION FUNCTIONS - ELLA
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


# ============================================================================
# CALCULATION FUNCTIONS - EMMA
# ============================================================================

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


# ============================================================================
# CALCULATION FUNCTIONS - MINDY
# ============================================================================

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
# CALCULATION FUNCTIONS - EVERYONE (Safety Score)
# ============================================================================

def calculate_safety_score(db_conn):
    """
    Calculates composite outdoor activity safety score for each city.
    Lower score = safer for outdoor activities.
    """
    cur = db_conn.cursor()
    
    # Get all cities that have all three types of data
    cur.execute('SELECT city_id, city_name FROM Cities')
    cities = cur.fetchall()
    
    safety_scores = {}
    
    for city_id, city_name in cities:
        # Get average temp for this city
        cur.execute('''
            SELECT AVG(temperature) 
            FROM Weather_Data 
            WHERE city_id = ?
        ''', (city_id,))
        temp_result = cur.fetchone()
        
        # Get average UV for this city
        cur.execute('''
            SELECT AVG(uv_index) 
            FROM UV_Data 
            WHERE city_id = ?
        ''', (city_id,))
        uv_result = cur.fetchone()
        
        # Get average AQI for this city
        cur.execute('''
            SELECT AVG(aqi_value) 
            FROM Air_Quality_Data 
            WHERE city_id = ?
        ''', (city_id,))
        aqi_result = cur.fetchone()
        
        # Only calculate safety score if city has all three data types
        if temp_result[0] and uv_result[0] and aqi_result[0]:
            avg_temp = temp_result[0]
            avg_uv = uv_result[0]
            avg_aqi = aqi_result[0]
            
            # Calculate normalized scores
            temp_score = abs(avg_temp - 70) / 30.0
            uv_score = avg_uv / 12.0
            aqi_score = avg_aqi / 6.0
            
            # Composite score
            composite_score = (temp_score * 0.3) + (uv_score * 0.3) + (aqi_score * 0.4)
            
            safety_scores[city_name] = composite_score
    
    # Sort by safety score using a simple bubble sort
    sorted_list = []
    for city, score in safety_scores.items():
        sorted_list.append((city, score))
    
    # Bubble sort
    for i in range(len(sorted_list)):
        for j in range(len(sorted_list) - 1 - i):
            if sorted_list[j][1] > sorted_list[j + 1][1]:
                sorted_list[j], sorted_list[j + 1] = sorted_list[j + 1], sorted_list[j]
    
    # Write to file
    with open(OUTPUT_FILE, 'a') as f:
        f.write("\n" + "="*50 + "\n")
        f.write("OUTDOOR ACTIVITY SAFETY SCORES\n")
        f.write("(Lower score = safer for outdoor activities)\n")
        f.write("="*50 + "\n\n")
        f.write(f"{'Rank':<6} {'City':<25} {'Safety Score':<15}\n")
        f.write("-" * 50 + "\n")
        
        for rank, (city, score) in enumerate(sorted_list, 1):
            f.write(f"{rank:<6} {city:<25} {score:.4f}\n")
            print(f"{rank}. {city}: {score:.4f}")
    
    return dict(sorted_list)


# ============================================================================
# DATA RETRIEVAL FOR VISUALIZATIONS - EVERYONE
# ============================================================================

def get_calculated_data(db_conn):
    """Retrieves all calculated data needed for visualizations."""
    cur = db_conn.cursor()
    
    # Get all cities
    cur.execute('SELECT city_id, city_name FROM Cities')
    cities_data = cur.fetchall()
    
    cities = []
    avg_temps = []
    avg_uvs = []
    avg_aqis = []
    safety_scores = []
    
    for city_id, city_name in cities_data:
        # Get average temp
        cur.execute('SELECT AVG(temperature) FROM Weather_Data WHERE city_id = ?', (city_id,))
        temp_result = cur.fetchone()
        
        # Get average UV
        cur.execute('SELECT AVG(uv_index) FROM UV_Data WHERE city_id = ?', (city_id,))
        uv_result = cur.fetchone()
        
        # Get average AQI
        cur.execute('SELECT AVG(aqi_value) FROM Air_Quality_Data WHERE city_id = ?', (city_id,))
        aqi_result = cur.fetchone()
        
        # Only include cities with all three data types
        if temp_result[0] and uv_result[0] and aqi_result[0]:
            avg_temp = temp_result[0]
            avg_uv = uv_result[0]
            avg_aqi = aqi_result[0]
            
            # Calculate safety score
            temp_score = abs(avg_temp - 70) / 30.0
            uv_score = avg_uv / 12.0
            aqi_score = avg_aqi / 6.0
            composite_score = (temp_score * 0.3) + (uv_score * 0.3) + (aqi_score * 0.4)
            
            cities.append(city_name)
            avg_temps.append(avg_temp)
            avg_uvs.append(avg_uv)
            avg_aqis.append(avg_aqi)
            safety_scores.append(composite_score)
    
    return {
        'cities': cities,
        'safety_scores': safety_scores,
        'avg_temps': avg_temps,
        'avg_uv': avg_uvs,
        'avg_aqi': avg_aqis
    }

# VISUALIZATION FUNCTIONS - EVERYONE

def create_safety_ranking_chart(calculated_data):
    """Creates ranked bar chart of top 10 safest cities."""
    # Sort by safety score and get top 10
    city_score_pairs = []
    for i in range(len(calculated_data['cities'])):
        city_score_pairs.append((calculated_data['cities'][i], calculated_data['safety_scores'][i]))
    
    # Sort by score (ascending - lower is better)
    city_score_pairs.sort(key=lambda x: x[1])
    
    # Get top 10
    top_cities = [pair[0] for pair in city_score_pairs[:10]]
    top_scores = [pair[1] for pair in city_score_pairs[:10]]
    
    plt.figure(figsize=(12, 6))
    colors = plt.cm.viridis(np.linspace(0, 0.8, len(top_cities)))
    plt.bar(range(len(top_cities)), top_scores, color=colors)
    plt.xlabel('City', fontsize=12, fontweight='bold')
    plt.ylabel('Safety Score (Lower = Safer)', fontsize=12, fontweight='bold')
    plt.title('Top 10 Safest Cities for Outdoor Activities', fontsize=14, fontweight='bold')
    plt.xticks(range(len(top_cities)), top_cities, rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('safety_ranking.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Created safety_ranking.png")



def create_grouped_comparison_chart(calculated_data):
    """Creates grouped bar chart comparing temperature, UV, and AQI."""
    # Select 10 cities with best safety scores
    data_tuples = []
    for i in range(len(calculated_data['cities'])):
        data_tuples.append((
            calculated_data['cities'][i],
            calculated_data['safety_scores'][i],
            calculated_data['avg_temps'][i],
            calculated_data['avg_uv'][i],
            calculated_data['avg_aqi'][i]
        ))
    
    # Sort by safety score
    data_tuples.sort(key=lambda x: x[1])
    
    # Get top 10
    cities = [t[0] for t in data_tuples[:10]]
    temps = [t[2] for t in data_tuples[:10]]
    uvs = [t[3] for t in data_tuples[:10]]
    aqis = [t[4] for t in data_tuples[:10]]
    
    x = np.arange(len(cities))
    width = 0.25
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Normalize for visual comparison
    temps_norm = [t / 100 * 10 for t in temps]  # Scale to similar range
    
    ax.bar(x - width, temps_norm, width, label='Temp (scaled)', color='#FF6B6B')
    ax.bar(x, uvs, width, label='UV Index', color='#4ECDC4')
    ax.bar(x + width, aqis, width, label='AQI', color='#95E1D3')
    
    ax.set_xlabel('City', fontsize=12, fontweight='bold')
    ax.set_ylabel('Values', fontsize=12, fontweight='bold')
    ax.set_title('Comparison of Weather Metrics (Top 10 Safest Cities)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(cities, rotation=45, ha='right')
    ax.legend()
    plt.tight_layout()
    plt.savefig('grouped_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Created grouped_comparison.png")


def create_scatter_plot(calculated_data):
    """Creates scatter plot of temperature vs AQI with UV as color."""
    plt.figure(figsize=(12, 8))
    
    scatter = plt.scatter(
        calculated_data['avg_temps'],
        calculated_data['avg_aqi'],
        c=calculated_data['avg_uv'],
        s=200,
        cmap='YlOrRd',
        alpha=0.6,
        edgecolors='black',
        linewidth=1.5
    )
    
    # Add city labels
    for i, city in enumerate(calculated_data['cities']):
        plt.annotate(city, 
                    (calculated_data['avg_temps'][i], calculated_data['avg_aqi'][i]),
                    fontsize=8,
                    alpha=0.7)
    
    plt.colorbar(scatter, label='UV Index')
    plt.xlabel('Average Temperature (°F)', fontsize=12, fontweight='bold')
    plt.ylabel('Average AQI', fontsize=12, fontweight='bold')
    plt.title('Temperature vs Air Quality (Color = UV Index)', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('scatter_temp_aqi.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Created scatter_temp_aqi.png")


def create_horizontal_rankings(calculated_data):
    """Creates three separate horizontal bar charts for individual rankings."""
    
    # Temperature ranking (closest to 70°F is best)
    temp_data = []
    for i in range(len(calculated_data['cities'])):
        temp_deviation = abs(calculated_data['avg_temps'][i] - 70)
        temp_data.append((calculated_data['cities'][i], calculated_data['avg_temps'][i], temp_deviation))
    
    # Sort by deviation (ascending)
    temp_data.sort(key=lambda x: x[2])
    
    # Get top 10
    cities_temp = [t[0] for t in temp_data[:10]]
    temps_sorted = [t[1] for t in temp_data[:10]]
    
    plt.figure(figsize=(10, 6))
    plt.barh(range(len(cities_temp)), temps_sorted, color='#FF6B6B')
    plt.yticks(range(len(cities_temp)), cities_temp)
    plt.xlabel('Average Temperature (°F)', fontsize=12, fontweight='bold')
    plt.title('Cities Ranked by Most Comfortable Temperature', fontsize=14, fontweight='bold')
    plt.axvline(x=70, color='green', linestyle='--', alpha=0.5, label='Ideal (70°F)')
    plt.legend()
    plt.tight_layout()
    plt.savefig('ranking_temperature.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # UV ranking (lower is better)
    uv_data = []
    for i in range(len(calculated_data['cities'])):
        uv_data.append((calculated_data['cities'][i], calculated_data['avg_uv'][i]))
    
    # Sort by UV (ascending)
    uv_data.sort(key=lambda x: x[1])
    
    # Get top 10
    cities_uv = [t[0] for t in uv_data[:10]]
    uvs_sorted = [t[1] for t in uv_data[:10]]
    
    plt.figure(figsize=(10, 6))
    plt.barh(range(len(cities_uv)), uvs_sorted, color='#4ECDC4')
    plt.yticks(range(len(cities_uv)), cities_uv)
    plt.xlabel('Average UV Index', fontsize=12, fontweight='bold')
    plt.title('Cities Ranked by Lowest UV Index', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('ranking_uv.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # AQI ranking (lower is better)
    aqi_data = []
    for i in range(len(calculated_data['cities'])):
        aqi_data.append((calculated_data['cities'][i], calculated_data['avg_aqi'][i]))
    
    # Sort by AQI (ascending)
    aqi_data.sort(key=lambda x: x[1])
    
    # Get top 10
    cities_aqi = [t[0] for t in aqi_data[:10]]
    aqis_sorted = [t[1] for t in aqi_data[:10]]
    
    plt.figure(figsize=(10, 6))
    plt.barh(range(len(cities_aqi)), aqis_sorted, color='#95E1D3')
    plt.yticks(range(len(cities_aqi)), cities_aqi)
    plt.xlabel('Average AQI', fontsize=12, fontweight='bold')
    plt.title('Cities Ranked by Best Air Quality', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('ranking_aqi.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Created ranking_temperature.png")
    print("✓ Created ranking_uv.png")
    print("✓ Created ranking_aqi.png")


def create_heatmap(calculated_data):
    """Creates color-coded heatmap grid of all metrics."""
    # Prepare data matrix
    all_data = []
    for i in range(len(calculated_data['cities'])):
        temps_norm = abs(calculated_data['avg_temps'][i] - 70) / 30
        uvs_norm = calculated_data['avg_uv'][i] / 12.0
        aqis_norm = calculated_data['avg_aqi'][i] / 6.0
        scores_norm = calculated_data['safety_scores'][i]
        
        all_data.append((
            calculated_data['cities'][i],
            calculated_data['safety_scores'][i],
            temps_norm,
            uvs_norm,
            aqis_norm,
            scores_norm
        ))
    
    # Sort by safety score
    all_data.sort(key=lambda x: x[1])
    
    # Extract sorted data
    cities_sorted = [t[0] for t in all_data]
    data_matrix = []
    for t in all_data:
        data_matrix.append([t[2], t[3], t[4], t[5]])  # temps_norm, uvs_norm, aqis_norm, scores_norm
    
    # Convert to numpy array for plotting
    data_matrix = np.array(data_matrix)
    
    fig, ax = plt.subplots(figsize=(10, 14))
    im = ax.imshow(data_matrix, cmap='RdYlGn_r', aspect='auto', vmin=0, vmax=1)
    
    # Set ticks and labels
    ax.set_xticks(np.arange(4))
    ax.set_yticks(np.arange(len(cities_sorted)))
    ax.set_xticklabels(['Temp\nDeviation', 'UV\nIndex', 'Air\nQuality', 'Safety\nScore'])
    ax.set_yticklabels(cities_sorted)
    
    # Rotate the tick labels for better readability
    plt.setp(ax.get_xticklabels(), rotation=0, ha="center", rotation_mode="anchor")
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Normalized Value (Green=Better, Red=Worse)', rotation=270, labelpad=20)
    
    # Add title
    ax.set_title('Heatmap of All Weather Metrics by City\n(Cities ranked by safety score)', 
                 fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    plt.savefig('heatmap_all_metrics.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Created heatmap_all_metrics.png")


def create_visualizations(calculated_data):
    """Master function that generates all visualizations."""
    print("\n" + "="*50)
    print("CREATING VISUALIZATIONS")
    print("="*50)
    
    if not calculated_data['cities']:
        print("No data available for visualizations")
        return
    
    create_safety_ranking_chart(calculated_data)
    create_grouped_comparison_chart(calculated_data)
    create_scatter_plot(calculated_data)
    create_horizontal_rankings(calculated_data)
    create_heatmap(calculated_data)
    
    print("\n✓ All visualizations created successfully!")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function that coordinates the entire program.
    """
    print("="*60)
    print("WEATHER DATA ANALYSIS PROJECT")
    print("="*60)
    
    print("\nInitializing database...")
    init_database()
    
    # Collect weather data (Ella's part)
    print("\n" + "="*50)
    print("COLLECTING WEATHER DATA (Ella)")
    print("="*50)
    store_weather(CITIES, OPENWEATHER_API_KEY)
    
    # Collect UV data (Emma's part)
    print("\n" + "="*50)
    print("COLLECTING UV DATA (Emma)")
    print("="*50)
    store_uv(CITIES, OPENUV_API_KEY, CITY_COORDS)
    
    # Collect air quality data (Mindy's part)
    print("\n" + "="*50)
    print("COLLECTING AIR QUALITY DATA (Mindy)")
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
    print("\nCalculating average temperature...")
    avg_temp = calculate_avg_temp(conn)
    
    print("Calculating average UV index...")
    avg_uv = calculate_avg_uv(conn)
    
    print("Calculating average AQI...")
    avg_aqi = calculate_avg_aqi(conn)
    
    print("\nCalculating safety scores...")
    safety_scores = calculate_safety_score(conn)
    
    # Display results
    print("\n" + "="*50)
    print("SUMMARY RESULTS")
    print("="*50)
    print(f"Overall Average Temperature: {avg_temp:.2f}°F" if avg_temp else "No temperature data")
    print(f"Overall Average UV Index: {avg_uv:.2f}" if avg_uv else "No UV data")
    print(f"Overall Average AQI: {avg_aqi:.2f}" if avg_aqi else "No AQI data")
    
    # Get data for visualizations
    print("\nRetrieving data for visualizations...")
    calculated_data = get_calculated_data(conn)
    
    # Create visualizations
    if calculated_data['cities']:
        create_visualizations(calculated_data)
    else:
        print("Insufficient data for visualizations. Run data collection multiple times over 4+ days.")
    
    conn.close()
    
    print("\n" + "="*50)
    print("COMPLETE!")
    print("="*50)
    print("✓ Check calculations_output.txt for detailed results")
    print("✓ Check PNG files for visualizations")
    print("="*50)


if __name__ == "__main__":
    main()