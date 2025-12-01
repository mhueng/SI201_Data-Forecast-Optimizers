import requests
import sqlite3
import json
import time

def store_air_quality(city_names, api_key, db_name = 'weather_data.db'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    cur.execute('''
                CREATE TABLE IF NOT EXISTS Cities (
                    city_id INTEGER PRIMARY KEY,
                    city_name TEXT UNIQUE
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
    
    store_count = 0
    max_stores = 25
    
    base_url = "http://api.weatherapi.com/v1/current.json"
    
    for city in city_names:
        if store_count >= max_stores:
            print(f"Reached limit of {max_stores} cities per run")
            break
        
        try:
            cur.execute('''
                        SELECT COUNT (*) FROM Air_Quality_Data
                        JOIN Cities ON Air_Quality_Data
                        JOIN Cities ON Air_Quality_Data.city_id = Cities.city_id
                        Where Cities.city_name = ? AND DATE(timestamp) = DATE('now')
                        ''', (city,))
            
            if cur.fetchone()[0] > 0:
                print(f"Air quality data for {city} already exists for today, skipping...")
                continue
            
            params = {
                'key': api_key,
                'q': city,
                'aqi': 'yes'
            }
            
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            aqi_value = data['current']['air_quality']['us-epa-index']
            timestamp = data['current']['last_updated']
            
            cur.execute('SELECT city_id FROM Cities WHERE city_name = ?', (city,))
            result= cur.fetchone()
            
            if result:
                city_id = result[0]
            else:
                cur.execute('SELECT MAX(city_id)FROM Cities')
                max_id = cur.fetchone()[0]
                city_id = 1 if max_id is None else max_id + 1
                cur.execute('INSERT INTO Cities (city_id, city_name) VALUES (?, ?)', (city_id, city))
                
            cur.execute('SELECT MAX(id) FROM Air_Quality_Data')
            max_id = cur.fetchone()[0]
            aqi_id = 1 if max_id is None else max_id + 1
            
            cur.execute('''
                        INSERT INTO Air_Quality_Data (id, city_id, aqi_value, timestamp)
                        VALUES (?, ?, ?, ?)
                        ''', (aqi_id, city_id, aqi_value, timestamp))
            
            conn.commit()
            stored_count += 1
            print(f'Stored air quality data for {city}: AQI = {aqi_value}')