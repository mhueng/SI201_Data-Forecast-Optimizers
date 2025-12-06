# ============================================================================
# DATA COLLECTION FUNCTIONS - ELLA
# ============================================================================

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

def store_weather(city_names, api_key, db_name):
    """
    Fetches weather data from OpenWeatherMap API and stores in database.
    
    Input: 
        - city_names: List of 25 city names (list of strings)
        - api_key: OpenWeatherMap API key (string)
        - db_name: Database name (string)
    
    Output: 
        - Returns count of successfully stored cities (integer)
    
    Process:
    - Connects to database
    - Limits to 25 items per execution
    - Checks for existing data to avoid duplicates
    - Fetches temperature and weather_condition from API
    - Stores data with timestamp in Weather_Data table
    - Links to Cities table via city_id
    """
def store_weather(city_names, api_key, db_name):
    """
    Fetches weather data from OpenWeatherMap API and stores in database.
    Returns the number of NEW rows inserted (max 25 per run).
    """

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    inserted_count = 0

    for city in city_names:
        if inserted_count >= 25:  # <-- satisfies “max 25 items per run” requirement
            break

        # Get coordinates for this city
        if city not in CITY_COORDS:
            print(f"Skipping {city}: no coordinates defined.")
            continue

        lat, lon = CITY_COORDS[city]

        # Get or create its city_id in Cities table
        city_id = get_or_create_city_id(cur, city)

        # Avoid duplicate data for the same city/day
        cur.execute("""
            SELECT 1 FROM Weather_Data
            WHERE city_id = ? AND date(timestamp) = date('now')
        """, (city_id,))
        if cur.fetchone():
            print(f"Already have data for {city} today; skipping.")
            continue

        # Call the OpenWeather API using lat/lon
        params = {
            "lat": lat,
            "lon": lon,
            "appid": api_key,
            "units": "imperial"  # Fahrenheit; use "metric" for °C if you prefer
        }

        try:
            response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"API error for {city}: {e}")
            continue

        data = response.json()

        # Parse out what you need
        temp = data["main"]["temp"]                      # float
        condition = data["weather"][0]["main"]           # e.g. "Clear", "Clouds"
        # Use OpenWeather's timestamp (Unix seconds) and convert to readable string
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(data["dt"]))

        # Insert row; UNIQUE(city_id, timestamp) prevents duplicates
        cur.execute("""
            INSERT OR IGNORE INTO Weather_Data
            (city_id, temperature, weather_condition, timestamp)
            VALUES (?, ?, ?, ?)
        """, (city_id, temp, condition, ts))

        # Only count if a new row was actually inserted
        if cur.rowcount > 0:
            inserted_count += 1
            print(f"Stored weather for {city} at {ts}")
        else:
            print(f"Duplicate row for {city} at {ts}, ignored.")

        # Be nice to the API
        time.sleep(1)

    conn.commit()
    conn.close()
    return inserted_count


# ============================================================================
# CALCULATION FUNCTIONS - ELLA
# ============================================================================
import sqlite3

def calculate_avg_temp(db_conn, city_id=None):
    """
    Calculates average temperature from weather data.
    
    Input: 
        - db_conn: Database connection object (sqlite3.Connection)
        - city_id: Optional specific city ID (integer or None)
    
    Output: 
        - Average temperature (float)
        - Writes result to calculations_output.txt
    
    Process:
    - Selects temperature data from Weather_Data table
    - Filters by city_id if provided, otherwise calculates for all cities
    - Computes average
    - Writes formatted result to output file
    """
    cur = db_conn.cursor()

    if city_id is None:
        cur.execute("SELECT AVG(temperature) FROM Weather_Data")
        label = "all cities"
    else:
        cur.execute("""
            SELECT AVG(temperature) FROM Weather_Data
            WHERE city_id = ?
        """, (city_id,))
        label = f"city_id={city_id}"

    result = cur.fetchone()[0]

    with open("calculations_output.txt", "a") as f:
        if result is None:
            f.write(f"No temperature data available for {label}.\n")
        else:
            f.write(f"Average temperature for {label}: {result:.2f}°F\n")

    return result
