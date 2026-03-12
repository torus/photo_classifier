import sqlite3
from pathlib import Path
from datetime import datetime

# Configuration
DB_PATH = Path('/classified/photos.db')

# Milan coordinates and search radius
MILAN_LAT = 45.4642
MILAN_LON = 9.1900
SEARCH_RADIUS_KM = 15  # Search within 15km of Milan center

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two coordinates in kilometers"""
    from math import radians, sin, cos, sqrt, atan2
    
    R = 6371  # Earth's radius in kilometers
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
dlon = lon2 - lon1
    
a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = R * c
    
    return distance

def query_photos_by_city(city_name, center_lat, center_lon, radius_km):
    """Query photos taken in a specific city by geographic coordinates"""
    try:
        if not DB_PATH.exists():
            print(f"Error: Database not found at {DB_PATH}")
            return
        
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Get all photos with GPS data
            cursor.execute('''
                SELECT id, filename, new_path, taken_date, latitude, longitude, camera_model
                FROM photos
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY taken_date DESC
            ''')
            
            all_photos = cursor.fetchall()
            
            if not all_photos:
                print(f"No photos with GPS data found in database.")
                return
            
            # Filter photos by distance from city center
            city_photos = []
            for photo in all_photos:
                photo_id, filename, path, taken_date, lat, lon, camera_model = photo
                distance = haversine_distance(center_lat, center_lon, lat, lon)
                
                if distance <= radius_km:
                    city_photos.append({
                        'id': photo_id,
                        'filename': filename,
                        'path': path,
                        'taken_date': taken_date,
                        'latitude': lat,
                        'longitude': lon,
                        'camera_model': camera_model,
                        'distance_km': round(distance, 2)
                    })
            
            # Display results
            print(f"\n{'='*80}")
            print(f"Photos taken in {city_name}")
            print(f"Center: ({center_lat}, {center_lon}) | Search radius: {radius_km}km")
            print(f"{'='*80}\n")
            
            if not city_photos:
                print(f"No photos found in {city_name} within {radius_km}km radius.")
                return
            
            print(f"Found {len(city_photos)} photo(s) in {city_name}:\n")
            
            for i, photo in enumerate(city_photos, 1):
                print(f"[{i}] {photo['filename']}")
                print(f"    ID: {photo['id']}")
                print(f"    Path: {photo['path']}")
                print(f"    Date: {photo['taken_date']}")
                print(f"    Coordinates: ({photo['latitude']}, {photo['longitude']})")
                print(f"    Distance from center: {photo['distance_km']}km")
                if photo['camera_model']:
                    print(f"    Camera: {photo['camera_model']}")
                print() 
    except Exception as e:
        print(f"Error querying database: {e}")

if __name__ == '__main__':
    # Query photos taken in Milan
    query_photos_by_city('Milan', MILAN_LAT, MILAN_LON, SEARCH_RADIUS_KM)