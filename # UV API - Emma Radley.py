# UV API - Emma Radley.py

import sqlite3
import requests
import time
from datetime import datetime

def create_uv_table(db_name):
    
    """
    Creates the UV_Data table in the database if it doesn't exist.
    
    Input:
        - db_name: Database name (string)
    
    Process:
    - Connects to database
    - Creates UV_Data table with proper schema
    - Links to Cities table via city_id foreign key
    """

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

 # Create UV_Data table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS UV_Data (
            uv_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL,
            uv_index REAL NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (city_id) REFERENCES Cities(city_id)
        )
    """)
    
    conn.commit()
    conn.close()
    print("UV_Data table created successfully!")

def store_uv(city_names, api_key, city_coordinates, db_name):
    """
    Fetches UV index data from OpenUV API and stores in database.
    
    Input: 
        - city_names: List of 25 city names (list of strings)
        - api_key: OpenUV API key (string)
        - city_coordinates: Dictionary mapping city names to (lat, lon) tuples (dict)
        - db_name: Database name (string)
    
    Output: 
        - Returns count of successfully stored cities (integer)
    
    Process:
    - Connects to database
    - Limits to 25 items per execution
    - Checks for existing data to avoid duplicates
    - Fetches UV index using coordinates from API
    - Stores data with timestamp in UV_Data table
    - Links to Cities table via city_id
    """
    
    # Limit to 25 cities
    city_names = city_names[:25]
    
    # Connect to database
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    # Counter for successfully stored cities
    success_count = 0
    
    # OpenUV API endpoint
    base_url = "https://api.openuv.io/api/v1/uv"
    
    for city_name in city_names:
        try:
            # Get city_id from Cities table
            cur.execute("SELECT city_id FROM Cities WHERE city_name = ?", (city_name,))
            result = cur.fetchone()
            
            if not result:
                print(f"City '{city_name}' not found in Cities table. Skipping.")
                continue
            
            city_id = result[0]
            
            # Check if UV data already exists for this city
            cur.execute("SELECT COUNT(*) FROM UV_Data WHERE city_id = ?", (city_id,))
            if cur.fetchone()[0] > 0:
                print(f"UV data already exists for '{city_name}'. Skipping.")
                continue
            
            # Get coordinates
            if city_name not in city_coordinates:
                print(f"Coordinates not found for '{city_name}'. Skipping.")
                continue
            
            lat, lon = city_coordinates[city_name]
            
            # Prepare API request
            headers = {
                "x-access-token": api_key
            }
            params = {
                "lat": lat,
                "lng": lon
            }
            
            # Fetch UV data from API
            response = requests.get(base_url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract UV index from response
                uv_index = data['result']['uv']
                
                # Get current timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Insert into UV_Data table
                cur.execute("""
                    INSERT INTO UV_Data (city_id, uv_index, timestamp)
                    VALUES (?, ?, ?)
                """, (city_id, uv_index, timestamp))
                
                conn.commit()
                success_count += 1
                print(f"Successfully stored UV data for '{city_name}' (UV: {uv_index})")
                
            else:
                print(f"API request failed for '{city_name}': {response.status_code}")
            
            # Rate limiting - be respectful to the API
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f"Network error for '{city_name}': {e}")
        except sqlite3.Error as e:
            print(f"Database error for '{city_name}': {e}")
            conn.rollback()
        except KeyError as e:
            print(f"Unexpected API response format for '{city_name}': {e}")
        except Exception as e:
            print(f"Unexpected error for '{city_name}': {e}")
    
    # Close database connection
    conn.close()
    
    print(f"\nTotal cities processed successfully: {success_count}/{len(city_names)}")
    return success_count


# Example usage:
if __name__ == "__main__":
    # Database name
    db_name = "outdoor_activity.db"
    
    # STEP 1: Create the UV_Data table first
    create_uv_table(db_name)
    
    # STEP 2: Prepare your data
    sample_cities = ["New York", "Los Angeles", "Chicago"]
    sample_api_key = "your_openuv_api_key_here"
    sample_coordinates = {
        "New York": (40.7128, -74.0060),
        "Los Angeles": (34.0522, -118.2437),
        "Chicago": (41.8781, -87.6298)
    }
