"""Find photos taken in or near a city.

Examples:
    python query_photos_by_city.py Valencia
    python query_photos_by_city.py "Valencia, Spain" --radius 30
    python query_photos_by_city.py Home --lat 35.68 --lon 139.77
"""
import argparse
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from collections import Counter
from math import asin, cos, radians, sin, sqrt
from pathlib import Path

DB_PATH = Path(os.environ.get('DB_PATH', '/classified/photos.db'))
GEOCODER_URL = 'https://nominatim.openstreetmap.org/search'
USER_AGENT = 'photo_classifier/1.0'


def haversine_distance(lat1, lon1, lat2, lon2):
    """Distance between two coordinates in kilometers"""
    lat1, lon1, lat2, lon2 = map(radians, (lat1, lon1, lat2, lon2))
    a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
    return 6371 * 2 * asin(sqrt(a))


def geocode(city_name):
    """Look up a city's coordinates with OpenStreetMap Nominatim.

    Returns (latitude, longitude, display_name), or None if not found.
    """
    query = urllib.parse.urlencode({'q': city_name, 'format': 'json', 'limit': 1})
    request = urllib.request.Request(f'{GEOCODER_URL}?{query}',
                                     headers={'User-Agent': USER_AGENT})
    with urllib.request.urlopen(request, timeout=10) as response:
        results = json.load(response)
    if not results:
        return None
    place = results[0]
    return float(place['lat']), float(place['lon']), place['display_name']


def query_photos_near(center_lat, center_lon, radius_km):
    """Return (distance_km, row) for photos within radius_km, oldest first"""
    # Cheap bounding box in SQL first, exact distance in Python after.
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * max(cos(radians(center_lat)), 0.01))
    with sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True) as conn:
        rows = conn.execute('''
            SELECT filename, new_path, taken_date, latitude, longitude, camera_model
            FROM photos
            WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?
            ORDER BY taken_date
        ''', (center_lat - dlat, center_lat + dlat,
              center_lon - dlon, center_lon + dlon)).fetchall()
    results = []
    for row in rows:
        distance = haversine_distance(center_lat, center_lon, row[3], row[4])
        if distance <= radius_km:
            results.append((distance, row))
    return results


def main():
    parser = argparse.ArgumentParser(description='Find photos taken in or near a city')
    parser.add_argument('city', help='city name, e.g. "Valencia" or "Valencia, Spain"')
    parser.add_argument('--radius', type=float, default=15,
                        help='search radius in km (default: 15)')
    parser.add_argument('--lat', type=float, help='center latitude (skips the lookup)')
    parser.add_argument('--lon', type=float, help='center longitude (skips the lookup)')
    args = parser.parse_args()

    if not DB_PATH.exists():
        sys.exit(f'Error: database not found at {DB_PATH}')

    if (args.lat is None) != (args.lon is None):
        sys.exit('Error: give both --lat and --lon, or neither')
    if args.lat is not None:
        center_lat, center_lon, place = args.lat, args.lon, args.city
    else:
        try:
            found = geocode(args.city)
        except Exception as e:
            sys.exit(f'Error: could not look up "{args.city}": {e}\n'
                     f'Use --lat and --lon to give the coordinates directly.')
        if not found:
            sys.exit(f'Error: no place found for "{args.city}"')
        center_lat, center_lon, place = found

    photos = query_photos_near(center_lat, center_lon, args.radius)

    print(f'{place}')
    print(f'Center: ({center_lat:.4f}, {center_lon:.4f}) | Radius: {args.radius:g} km')
    print(f'Found {len(photos)} photo(s)')
    if not photos:
        return

    print('\nBy day:')
    for day, count in sorted(Counter(row[2][:10] for _, row in photos).items()):
        print(f'  {day}  {count}')

    print('\nPhotos:')
    for distance, (filename, path, taken_date, lat, lon, camera) in photos:
        print(f'  {taken_date}  {distance:5.1f} km  {path}')


if __name__ == '__main__':
    main()
