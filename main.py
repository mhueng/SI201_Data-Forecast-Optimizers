import requests
import sqlite3
import time
from datetime import datetime


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


OPENWEATHER_API_KEY = get_api_key('openweather_api_key.txt')
OPENUV_API_KEY = get_api_key('openuv_api_key.txt')
WEATHERAPI_KEY = get_api_key('weatherapi_api_key.txt')

OPENWEATHER_BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'
OPENUV_BASE_URL = 'https://api.openuv.io/api/v1/uv'
WEATHERAPI_BASE_URL = 'http://api.weatherapi.com/v1/current.json'

DB_NAME = 'weather_data.db'

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
    "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "Indianapolis",
    "San Francisco", "Seattle", "Denver", "Boston", "Nashville",
    "Detroit", "Portland", "Las Vegas", "Memphis", "Louisville"
]

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


def init_database():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Cities (
            city_id INTEGER PRIMARY KEY,
            city_name TEXT UNIQUE
        )
    ''')
    
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
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS UV_Data (
            id INTEGER PRIMARY KEY,
            city_id INTEGER,
            uv_index REAL,
            timestamp TEXT,
            FOREIGN KEY (city_id) REFERENCES Cities(city_id)
        )
    ''')
    
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


def store_weather(city_names, api_key, db_name=DB_NAME):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    store_count = 0
    max_stores = 25
    
    for city in city_names:
        if store_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
            params = {
                'q': city,
                'appid': api_key,
                'units': 'imperial'
            }
            
            response = requests.get(OPENWEATHER_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            temperature = data['main']['temp']
            weather_condition = data['weather'][0]['main']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            
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
            
            cur.execute('SELECT MAX(id) FROM Weather_Data')
            max_id = cur.fetchone()[0]
            weather_id = 1 if max_id is None else max_id + 1
            
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
    print(f"\nTotal weather records stored this run: {store_count}")
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM Weather_Data')
    total = cur.fetchone()[0]
    conn.close()
    print(f"Total weather records in database: {total}")
    
    return store_count


def store_uv(city_names, api_key, city_coordinates, db_name=DB_NAME):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    stored_count = 0
    max_stores = 25
    
    for city in city_names:
        if stored_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
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
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            
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
            
            cur.execute('SELECT MAX(id) FROM UV_Data')
            max_id = cur.fetchone()[0]
            uv_id = 1 if max_id is None else max_id + 1
            
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
    print(f"\nTotal UV records stored this run: {stored_count}")
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM UV_Data')
    total = cur.fetchone()[0]
    conn.close()
    print(f"Total UV records in database: {total}")
    
    return stored_count


def store_air_quality(city_names, api_key, db_name=DB_NAME):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    store_count = 0
    max_stores = 25
    
    for city in city_names:
        if store_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
            params = {
                'key': api_key,
                'q': city,
                'aqi': 'yes'
            }
            
            response = requests.get(WEATHERAPI_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            aqi_value = data['current']['air_quality']['us-epa-index']
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            
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
            
            cur.execute('SELECT MAX(id) FROM Air_Quality_Data')
            max_id = cur.fetchone()[0]
            aqi_id = 1 if max_id is None else max_id + 1
            
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
    print(f"\nTotal air quality records stored this run: {store_count}")
    
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM Air_Quality_Data')
    total = cur.fetchone()[0]
    conn.close()
    print(f"Total air quality records in database: {total}")
    
    return store_count


def main():
    print("="*60)
    print("WEATHER DATA COLLECTION")
    print("="*60)
    
    print("\nInitializing database...")
    init_database()
    
    print("\n" + "="*50)
    print("COLLECTING WEATHER DATA (Ella)")
    print("="*50)
    store_weather(CITIES, OPENWEATHER_API_KEY)
    
    print("\n" + "="*50)
    print("COLLECTING UV DATA (Emma)")
    print("="*50)
    store_uv(CITIES, OPENUV_API_KEY, CITY_COORDS)
    
    print("\n" + "="*50)
    print("COLLECTING AIR QUALITY DATA (Mindy)")
    print("="*50)
    store_air_quality(CITIES, WEATHERAPI_KEY)
    
    print("\n" + "="*50)
    print("DATA COLLECTION COMPLETE!")
    print("="*50)


if __name__ == "__main__":
    main()
