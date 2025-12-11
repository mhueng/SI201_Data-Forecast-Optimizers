import sqlite3
import matplotlib.pyplot as plt
import numpy as np


DB_NAME = 'weather_data.db'
OUTPUT_FILE = 'calculations_output.txt'


def calculate_avg_temp(db_conn, city_id=None):
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


def calculate_safety_score(db_conn):
    cur = db_conn.cursor()
    
    cur.execute('SELECT city_id, city_name FROM Cities')
    cities = cur.fetchall()
    
    safety_scores = {}
    
    for city_id, city_name in cities:
        cur.execute('''
            SELECT AVG(temperature) 
            FROM Weather_Data 
            WHERE city_id = ?
        ''', (city_id,))
        temp_result = cur.fetchone()
        
        cur.execute('''
            SELECT AVG(uv_index) 
            FROM UV_Data 
            WHERE city_id = ?
        ''', (city_id,))
        uv_result = cur.fetchone()
        
        cur.execute('''
            SELECT AVG(aqi_value) 
            FROM Air_Quality_Data 
            WHERE city_id = ?
        ''', (city_id,))
        aqi_result = cur.fetchone()
        
        if temp_result[0] and uv_result[0] and aqi_result[0]:
            avg_temp = temp_result[0]
            avg_uv = uv_result[0]
            avg_aqi = aqi_result[0]
            
            temp_score = abs(avg_temp - 70) / 30.0
            uv_score = avg_uv / 12.0
            aqi_score = avg_aqi / 6.0
            
            composite_score = (temp_score * 0.3) + (uv_score * 0.3) + (aqi_score * 0.4)
            
            safety_scores[city_name] = composite_score
    
    sorted_list = []
    for city, score in safety_scores.items():
        sorted_list.append((city, score))
    
    for i in range(len(sorted_list)):
        for j in range(len(sorted_list) - 1 - i):
            if sorted_list[j][1] > sorted_list[j + 1][1]:
                sorted_list[j], sorted_list[j + 1] = sorted_list[j + 1], sorted_list[j]
    
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


def get_calculated_data(db_conn):
    cur = db_conn.cursor()
    
    cur.execute('SELECT city_id, city_name FROM Cities')
    cities_data = cur.fetchall()
    
    cities = []
    avg_temps = []
    avg_uvs = []
    avg_aqis = []
    safety_scores = []
    
    for city_id, city_name in cities_data:
        cur.execute('SELECT AVG(temperature) FROM Weather_Data WHERE city_id = ?', (city_id,))
        temp_result = cur.fetchone()
        
        cur.execute('SELECT AVG(uv_index) FROM UV_Data WHERE city_id = ?', (city_id,))
        uv_result = cur.fetchone()
        
        cur.execute('SELECT AVG(aqi_value) FROM Air_Quality_Data WHERE city_id = ?', (city_id,))
        aqi_result = cur.fetchone()
        
        if temp_result[0] and uv_result[0] and aqi_result[0]:
            avg_temp = temp_result[0]
            avg_uv = uv_result[0]
            avg_aqi = aqi_result[0]
            
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


def create_safety_ranking_chart(calculated_data):
    city_score_pairs = []
    for i in range(len(calculated_data['cities'])):
        city_score_pairs.append((calculated_data['cities'][i], calculated_data['safety_scores'][i]))
    
    city_score_pairs.sort(key=lambda x: x[1])
    
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
    data_tuples = []
    for i in range(len(calculated_data['cities'])):
        data_tuples.append((
            calculated_data['cities'][i],
            calculated_data['safety_scores'][i],
            calculated_data['avg_temps'][i],
            calculated_data['avg_uv'][i],
            calculated_data['avg_aqi'][i]
        ))
    
    data_tuples.sort(key=lambda x: x[1])
    
    cities = [t[0] for t in data_tuples[:10]]
    temps = [t[2] for t in data_tuples[:10]]
    uvs = [t[3] for t in data_tuples[:10]]
    aqis = [t[4] for t in data_tuples[:10]]
    
    x = np.arange(len(cities))
    width = 0.25
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    temps_norm = [t / 100 * 10 for t in temps]  
    
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
    
    temp_data = []
    for i in range(len(calculated_data['cities'])):
        temp_deviation = abs(calculated_data['avg_temps'][i] - 70)
        temp_data.append((calculated_data['cities'][i], calculated_data['avg_temps'][i], temp_deviation))
    
    temp_data.sort(key=lambda x: x[2])
    
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
    
    uv_data = []
    for i in range(len(calculated_data['cities'])):
        uv_data.append((calculated_data['cities'][i], calculated_data['avg_uv'][i]))
    
    uv_data.sort(key=lambda x: x[1])
    
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
    
    aqi_data = []
    for i in range(len(calculated_data['cities'])):
        aqi_data.append((calculated_data['cities'][i], calculated_data['avg_aqi'][i]))
    
    aqi_data.sort(key=lambda x: x[1])
    
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
    
    all_data.sort(key=lambda x: x[1])
    
    cities_sorted = [t[0] for t in all_data]
    data_matrix = []
    for t in all_data:
        data_matrix.append([t[2], t[3], t[4], t[5]])  
    
    data_matrix = np.array(data_matrix)
    
    fig, ax = plt.subplots(figsize=(10, 14))
    im = ax.imshow(data_matrix, cmap='RdYlGn_r', aspect='auto', vmin=0, vmax=1)
    
    ax.set_xticks(np.arange(4))
    ax.set_yticks(np.arange(len(cities_sorted)))
    ax.set_xticklabels(['Temp\nDeviation', 'UV\nIndex', 'Air\nQuality', 'Safety\nScore'])
    ax.set_yticklabels(cities_sorted)
    
    plt.setp(ax.get_xticklabels(), rotation=0, ha="center", rotation_mode="anchor")
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Normalized Value (Green=Better, Red=Worse)', rotation=270, labelpad=20)
    
    ax.set_title('Heatmap of All Weather Metrics by City\n(Cities ranked by safety score)', 
                 fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    plt.savefig('heatmap_all_metrics.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Created heatmap_all_metrics.png")


def create_visualizations(calculated_data):
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


def main():
    print("="*60)
    print("WEATHER DATA ANALYSIS - CALCULATIONS & VISUALIZATIONS")
    print("="*60)
    
    print("\n" + "="*50)
    print("PERFORMING CALCULATIONS")
    print("="*50)
    
    with open(OUTPUT_FILE, 'w') as f:
        f.write("WEATHER DATA ANALYSIS RESULTS\n")
        f.write("="*50 + "\n")
    
    conn = sqlite3.connect(DB_NAME)
    
    print("\nCalculating average temperature...")
    avg_temp = calculate_avg_temp(conn)
    
    print("Calculating average UV index...")
    avg_uv = calculate_avg_uv(conn)
    
    print("Calculating average AQI...")
    avg_aqi = calculate_avg_aqi(conn)
    
    print("\nCalculating safety scores...")
    safety_scores = calculate_safety_score(conn)
    
    print("\n" + "="*50)
    print("SUMMARY RESULTS")
    print("="*50)
    print(f"Overall Average Temperature: {avg_temp:.2f}°F" if avg_temp else "No temperature data")
    print(f"Overall Average UV Index: {avg_uv:.2f}" if avg_uv else "No UV data")
    print(f"Overall Average AQI: {avg_aqi:.2f}" if avg_aqi else "No AQI data")
    
    print("\nRetrieving data for visualizations...")
    calculated_data = get_calculated_data(conn)
    
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
