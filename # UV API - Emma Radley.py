# UV API - Emma Radley.py

import requests
import sqlite3
import time

def store_uv(city_names, api_key, city_coordinates, db_name='weather_data.db'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    cur.execute('''
                CREATE TABLE IF NOT EXISTS Cities (
                    city_id INTEGER PRIMARY KEY,
                    city_name TEXT UNIQUE
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
    
    conn.commit()
    
    stored_count = 0
    max_stores = 25
    
    base_url = "https://api.openuv.io/api/v1/uv"
    
    for city in city_names:
        if stored_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
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
            
            headers = {
                'x-access-token': api_key
            }
            
            params = {
                'lat': lat,
                'lng': lon
            }
            
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            uv_index = data['result']['uv']
            timestamp = data['result']['uv_time']
            
            cur.execute('SELECT city_id FROM Cities WHERE city_name = ?', (city,))
            result = cur.fetchone()
            
            if result:
                city_id = result[0]
            else:
                cur.execute('SELECT MAX(city_id) FROM Cities')
                max_id = cur.fetchone()[0]
                city_id = 1 if max_id is None else max_id + 1
                cur.execute('INSERT INTO Cities (city_id, city_name) VALUES (?, ?)', (city_id, city))
            
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
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {e}")
            continue
        except KeyError as e:
            print(f"Error parsing data for {city}: Missing key {e}")
            continue
    
    conn.close()
    print(f"\nTotal cities stored this run: {stored_count}")
    return stored_count


def calculate_avg_uv(conn, city_id=None):
    cur = conn.cursor()
    
    if city_id is None:
        cur.execute('''
                    SELECT Cities.city_name, AVG(UV_Data.uv_index) as avg_uv
                    FROM UV_Data
                    JOIN Cities ON UV_Data.city_id = Cities.city_id
                    GROUP BY Cities.city_id, Cities.city_name
                    ORDER BY avg_uv
                    ''')
        results = cur.fetchall()
        
        with open('calculations_output.txt', 'a') as f:
            f.write("\n" + "="*50 + "\n")
            f.write("AVERAGE UV INDEX BY CITY\n")
            f.write("="*50 + "\n")
            for city_name, avg_uv in results:
                f.write(f"{city_name}: {avg_uv:.2f}\n")
        
        print("\n=== Average UV Index by City ===")
        for city_name, avg_uv in results:
            print(f"{city_name}: {avg_uv:.2f}")
        
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
        
        cur.execute('SELECT city_name FROM Cities WHERE city_id = ?', (city_id,))
        city_name = cur.fetchone()[0]
        
        with open('calculations_output.txt', 'a') as f:
            f.write(f"\nAverage UV Index for {city_name}: {avg_uv:.2f}\n")
        
        print(f"Average UV Index for {city_name}: {avg_uv:.2f}")
        
        return avg_uv if avg_uv else 0.0


if __name__ == "__main__":
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
        "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "Indianapolis",
        "San Francisco", "Seattle", "Denver", "Boston", "Nashville",
        "Detroit", "Portland", "Las Vegas", "Memphis", "Louisville"
    ]
    
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
    
    api_key = "openuv-ac490wrminnpcmb-io"

    
    print("Running store_uv function...")
    count = store_uv(cities, api_key, city_coordinates)
    print(f"\n✓ Successfully stored UV data for {count} cities")
    
    # STEP 2: Calculate averages (run this AFTER collecting all data over 4 days)
    # Uncomment the lines below after you've collected all your data
    """
    print("\n" + "="*50)
    print("Calculating average UV index for all cities...")
    print("="*50)
    conn = sqlite3.connect('weather_data.db')
    avg_uv = calculate_avg_uv(conn)
    print(f"\n✓ Overall average UV index across all cities: {avg_uv:.2f}")
    print("✓ Results written to calculations_output.txt")
    conn.close()
    """
