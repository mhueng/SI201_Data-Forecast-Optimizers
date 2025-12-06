import requests
import sqlite3
import time

def store_weather(city_list, api_key):
    """
    Fetches batched weather data for up to 25 cities and inserts normalized rows 
    into the Weather_Data table without duplicates.
    
    Input: 
        - city_list: List of 25 city names (strings)
        - api_key: OpenWeatherMap API key (string)
    Output: 
        - Stores weather data in database
        - Returns count of stored cities (integer)
    """
    # Enforce maximum of 25 cities
    if len(city_list) > 25:
        print(f"Warning: List contains {len(city_list)} cities. Limiting to first 25.")
        city_list = city_list[:25]
    
    conn = sqlite3.connect('weather_data.db')
    cur = conn.cursor()
    
    # Create Cities table if it doesn't existå
    cur.execute('''CREATE TABLE IF NOT EXISTS Cities (
                    city_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_name TEXT UNIQUE,
                    latitude REAL,
                    longitude REAL
                )''')
    
    # Create Weather_Data table if it doesn't exist
    cur.execute('''CREATE TABLE IF NOT EXISTS Weather_Data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_id INTEGER,
                    temperature REAL,
                    weather_condition TEXT,
                    timestamp INTEGER,
                    FOREIGN KEY (city_id) REFERENCES Cities(city_id),
                    UNIQUE(city_id, timestamp)
                )''')
    
    # City coordinates (latitude, longitude)
    city_coordinates = {
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
        "Charlotte": (35.2271, -80.8431),
        "Seattle": (47.6062, -122.3321),
        "Denver": (39.7392, -104.9903),
        "Boston": (42.3601, -71.0589),
        "Portland": (45.5152, -122.6784),
        "Las Vegas": (36.1699, -115.1398),
        "Miami": (25.7617, -80.1918),
        "Atlanta": (33.7490, -84.3880),
        "Nashville": (36.1627, -86.7816),
        "Detroit": (42.3314, -83.0458),
        "Baltimore": (39.2904, -76.6122),
        "Minneapolis": (44.9778, -93.2650),
        "Tampa": (27.9506, -82.4572)
    }
    
    stored_count = 0
    
    for city_name in city_list:
        try:
            # Get coordinates for the city
            if city_name not in city_coordinates:
                print(f"✗ Coordinates not found for {city_name}")
                continue
            
            lat, lon = city_coordinates[city_name]
            
            # Insert or get city_id (normalized - no duplicates)
            cur.execute('INSERT OR IGNORE INTO Cities (city_name, latitude, longitude) VALUES (?, ?, ?)',
                       (city_name, lat, lon))
            cur.execute('SELECT city_id FROM Cities WHERE city_name = ?', (city_name,))
            city_id = cur.fetchone()[0]
            
            # Fetch weather data from API
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract relevant data
                temperature = data['main']['temp']
                weather_condition = data['weather'][0]['main']
                timestamp = data['dt']
                
                # Insert normalized row (UNIQUE constraint prevents duplicates)
                try:
                    cur.execute('''INSERT INTO Weather_Data 
                                  (city_id, temperature, weather_condition, timestamp) 
                                  VALUES (?, ?, ?, ?)''',
                               (city_id, temperature, weather_condition, timestamp))
                    stored_count += 1
                    print(f"✓ Stored weather data for {city_name}")
                except sqlite3.IntegrityError:
                    print(f"⊙ Duplicate entry for {city_name} at timestamp {timestamp} - skipped")
                
            else:
                print(f"✗ Failed to fetch data for {city_name}: {response.status_code}")
            
            # Small delay to respect API rate limits
            time.sleep(0.5)
            
        except Exception as e:
            print(f"✗ Error processing {city_name}: {str(e)}")
    
    conn.commit()
    conn.close()
    
    print(f"\nTotal cities stored: {stored_count}/{len(city_list)}")
    return stored_count


def calculate_avg_temp(db_connection, city_id=None):
    """
    Computes the average temperature per city from the database and appends 
    the result to calculations_output.txt.
    
    Input:
        - db_connection: sqlite3 database connection object
        - city_id: Specific city ID to calculate for, or None for all cities (integer or None)
    Output:
        - Average temperature (float), writes to calculations_output.txt
    """
    cur = db_connection.cursor()
    
    if city_id is None:
        # Calculate average for all cities
        cur.execute('''SELECT Cities.city_name, AVG(Weather_Data.temperature) as avg_temp
                      FROM Weather_Data
                      JOIN Cities ON Weather_Data.city_id = Cities.city_id
                      GROUP BY Cities.city_id, Cities.city_name
                      ORDER BY Cities.city_name''')
        results = cur.fetchall()
        
        # Append to output file
        with open('calculations_output.txt', 'a') as f:
            f.write("\n=== AVERAGE TEMPERATURE BY CITY ===\n")
            for city_name, avg_temp in results:
                f.write(f"{city_name}: {avg_temp:.2f}°C\n")
        
        # Return overall average
        cur.execute('SELECT AVG(temperature) FROM Weather_Data')
        overall_avg = cur.fetchone()[0]
        
        print(f"Average temperatures calculated and written to calculations_output.txt")
        return overall_avg if overall_avg else 0.0
        
    else:
        # Calculate average for specific city
        cur.execute('''SELECT Cities.city_name, AVG(Weather_Data.temperature) as avg_temp
                      FROM Weather_Data
                      JOIN Cities ON Weather_Data.city_id = Cities.city_id
                      WHERE Cities.city_id = ?
                      GROUP BY Cities.city_id''', (city_id,))
        result = cur.fetchone()
        
        if result:
            city_name, avg_temp = result
            with open('calculations_output.txt', 'a') as f:
                f.write(f"\nAverage temperature for {city_name}: {avg_temp:.2f}°C\n")
            
            print(f"Average temperature for {city_name} written to calculations_output.txt")
            return avg_temp
        else:
            print(f"No data found for city_id {city_id}")
            return 0.0


# Example usage
if __name__ == "__main__":
    # List of 25 city names
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
        "Jacksonville", "Fort Worth", "Charlotte", "Seattle", "Denver",
        "Boston", "Portland", "Las Vegas", "Miami", "Atlanta",
        "Nashville", "Detroit", "Baltimore", "Minneapolis", "Tampa"
    ]
    
    # Your API key
    API_KEY = "d2b6933ae0a235dbdb6209f0ac09cb8e"
    
    # Store weather data for all 25 cities
    count = store_weather(cities, API_KEY)
    
    # Calculate average temperatures
    conn = sqlite3.connect('weather_data.db')
    avg_temp = calculate_avg_temp(conn, city_id=None)
    print(f"\nOverall average temperature: {avg_temp:.2f}°C")
    conn.close()