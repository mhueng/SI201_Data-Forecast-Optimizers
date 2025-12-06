import requests
import sqlite3
import time

def store_weather(city_names, api_key, db_name='weather_data.db'):
    conn = sqlite3.connect(db_name)
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
    
    conn.commit()
    
    store_count = 0
    max_stores = 25
    
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    
    for city in city_names:
        if store_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
            cur.execute('''
                SELECT COUNT(*) FROM Weather_Data
                JOIN Cities ON Weather_Data.city_id = Cities.city_id
                WHERE Cities.city_name = ? AND DATE(timestamp) = DATE('now')
            ''', (city,))
            
            if cur.fetchone()[0] > 0:
                print(f"Weather data for {city} already exists for today, skipping...")
                continue
            
            params = {
                'q': city,
                'appid': api_key,
                'units': 'imperial'  
            }
            
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            temperature = data['main']['temp']
            weather_condition = data['weather'][0]['main']
            
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
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
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {e}")
            continue
        except KeyError as e:
            print(f"Error parsing data for {city}: Missing key {e}")
            continue
        except Exception as e:
            print(f"Unexpected error for {city}: {e}")
            continue
    
    conn.close()
    print(f"\nTotal cities stored this run: {store_count}")
    return store_count


def calculate_avg_temp(conn, city_id=None):
    
    cur = conn.cursor()
    
    with open('calculations_output.txt', 'a') as f:
        f.write("\n" + "="*50 + "\n")
        f.write("AVERAGE TEMPERATURE CALCULATIONS\n")
        f.write("="*50 + "\n\n")
        
        if city_id is None:
            # Calculate average temperature for all cities
            cur.execute('''
                SELECT Cities.city_name, AVG(Weather_Data.temperature) as avg_temp
                FROM Weather_Data
                JOIN Cities ON Weather_Data.city_id = Cities.city_id
                GROUP BY Cities.city_id, Cities.city_name
                ORDER BY avg_temp DESC
            ''')
            
            results = cur.fetchall()
            
            if not results:
                print("No weather data found in database")
                f.write("No weather data found in database\n")
                return None
            
            cur.execute('SELECT AVG(temperature) FROM Weather_Data')
            overall_avg = cur.fetchone()[0]
            
            f.write(f"Overall Average Temperature: {overall_avg:.2f}°F\n\n")
            f.write("Average Temperature by City:\n")
            f.write("-" * 40 + "\n")
            
            for city_name, avg_temp in results:
                f.write(f"{city_name:25s}: {avg_temp:.2f}°F\n")
                print(f"{city_name}: Average Temperature = {avg_temp:.2f}°F")
            
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
            
            if not result:
                print(f"No weather data found for city_id {city_id}")
                f.write(f"No weather data found for city_id {city_id}\n")
                return None
            
            city_name, avg_temp = result
            
            f.write(f"City: {city_name}\n")
            f.write(f"Average Temperature: {avg_temp:.2f}°F\n")
            
            cur.execute('''
                SELECT COUNT(*) FROM Weather_Data 
                WHERE city_id = ?
            ''', (city_id,))
            count = cur.fetchone()[0]
            
            f.write(f"Data points: {count}\n")
            
            print(f"{city_name}: Average Temperature = {avg_temp:.2f}°F (from {count} measurements)")
            
            return avg_temp


def get_temp_comfort_level(temp_fahrenheit):
    if temp_fahrenheit < 32:
        return "Freezing"
    elif 32 <= temp_fahrenheit < 50:
        return "Cold"
    elif 50 <= temp_fahrenheit < 65:
        return "Cool"
    elif 65 <= temp_fahrenheit < 75:
        return "Comfortable"
    elif 75 <= temp_fahrenheit < 85:
        return "Warm"
    elif 85 <= temp_fahrenheit < 95:
        return "Hot"
    else:
        return "Very Hot"


if __name__ == "__main__":
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
        "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "Indianapolis",
        "San Francisco", "Seattle", "Denver", "Boston", "Nashville",
        "Detroit", "Portland", "Las Vegas", "Memphis", "Louisville"
    ]
    
    # Replace with your OpenWeatherMap API key
    API_KEY = "d2b6933ae0a235dbdb6209f0ac09cb8e"
    
    # Run the storage function
    count = store_weather(cities, API_KEY)
    print(f"Successfully stored data for {count} cities")
    
    # After running storage multiple times to get 100+ entries,
    # you can run the calculation function:
    # conn = sqlite3.connect('weather_data.db')
    # overall_avg = calculate_avg_temp(conn, city_id=None)
    # if overall_avg:
    #     comfort = get_temp_comfort_level(overall_avg)
    #     print(f"\nOverall temperature comfort: {comfort}")
    # conn.close()