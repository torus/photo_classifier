import os
import sqlite3
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

try:
    from PIL import Image
    from PIL.ExifTags import TAGS
except ImportError:
    print("Error: Pillow not installed. Install with: pip install Pillow")
    exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PHOTOS_DIR = Path('/photos')
CLASSIFIED_DIR = Path('/classified')
DB_PATH = Path('/classified/photos.db')
MAX_WORKERS = 4

class PhotoClassifier:
    def __init__(self):
        self.db_path = DB_PATH
        self.init_database()
        
    def init_database(self):
        """Initialize SQLite database with photos table"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS photos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    original_path TEXT NOT NULL,
                    new_path TEXT NOT NULL,
                    taken_date TEXT,
                    latitude REAL,
                    longitude REAL,
                    camera_model TEXT,
                    processed_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
    
    def extract_exif(self, image_path):
        """Extract EXIF data from image, focusing on date and GPS"""
        exif_data = {
            'date': None,
            'latitude': None,
            'longitude': None,
            'camera_model': None
        }
        
        try:
            image = Image.open(image_path)
            exif = image._getexif()
            
            if not exif:
                return exif_data
            
            for tag_id, value in exif.items():
                tag_name = TAGS.get(tag_id, tag_id)
                
                # Extract datetime
                if tag_name == 'DateTime':
                    exif_data['date'] = value
                
                # Extract GPS latitude
                elif tag_name == 'GPSInfo':
                    gps_data = self.parse_gps(value)
                    if gps_data:
                        exif_data['latitude'] = gps_data[0]
                        exif_data['longitude'] = gps_data[1]
                
                # Extract camera model
                elif tag_name == 'Model':
                    exif_data['camera_model'] = value
            
        except Exception as e:
            logger.warning(f"Error reading EXIF from {image_path}: {e}")
        
        return exif_data
    
    def parse_gps(self, gps_info):
        """Parse GPS IFD to get latitude and longitude"""
        try:
            lat = self.convert_to_degrees(gps_info[2])
            lon = self.convert_to_degrees(gps_info[4])
            
            # Apply direction (N/S, E/W)
            if gps_info[1] == 'S':
                lat = -lat
            if gps_info[3] == 'W':
                lon = -lon
            
            return (lat, lon)
        except Exception as e:
            logger.warning(f"Error parsing GPS data: {e}")
            return None
    
    @staticmethod
    def convert_to_degrees(value):
        """Convert GPS coordinates to degrees"""
        d, m, s = value
        return d + (m / 60.0) + (s / 3600.0)
    
    def get_date_from_filename(self, filename):
        """Try to extract date from filename as fallback"""
        try:
            parts = filename.split('_')
            for part in parts:
                if len(part) == 8 and part.isdigit():
                    return datetime.strptime(part, '%Y%m%d').strftime('%Y-%m-%d')
        except:
            pass
        return None
    
    def process_photo(self, image_path):
        """Process a single photo: extract EXIF, classify by date, store metadata"""
        try:
            exif_data = self.extract_exif(image_path)
            taken_date = exif_data['date']
            
            # Fallback to filename if no EXIF date found
            if not taken_date:
                taken_date = self.get_date_from_filename(image_path.name)
            
            if not taken_date:
                logger.warning(f"No date found for {image_path.name}, skipping")
                return
            
            # Parse date string
            if isinstance(taken_date, str) and ':' in taken_date:
                date_obj = datetime.strptime(taken_date, '%Y:%m:%d %H:%M:%S')
            else:
                date_obj = datetime.strptime(taken_date, '%Y-%m-%d')
            
            # Create classified directory structure
            year = date_obj.strftime('%Y')
            month = date_obj.strftime('%m')
            day = date_obj.strftime('%d')
            
            target_dir = CLASSIFIED_DIR / year / month / day
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Move file
            new_path = target_dir / image_path.name
            shutil.move(str(image_path), str(new_path))
            
            # Store metadata in database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO photos 
                    (filename, original_path, new_path, taken_date, latitude, longitude, camera_model)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    image_path.name,
                    str(image_path),
                    str(new_path),
                    date_obj.isoformat(),
                    exif_data['latitude'],
                    exif_data['longitude'],
                    exif_data['camera_model']
                ))
                conn.commit()
            
            logger.info(f"Processed: {image_path.name} -> {new_path}")
            
        except Exception as e:
            logger.error(f"Error processing {image_path.name}: {e}")
    
    def run(self):
        """Main execution: scan photos directory and process all images"""
        if not PHOTOS_DIR.exists():
            logger.error(f"Photos directory not found: {PHOTOS_DIR}")
            return
        
        CLASSIFIED_DIR.mkdir(parents=True, exist_ok=True)
        
        # Memory-efficient directory scanning using scandir
        image_files = []
        supported_formats = ('.jpg', '.jpeg', '.heic', '.png', '.gif', '.bmp')
        
        for entry in os.scandir(PHOTOS_DIR):
            if entry.is_file() and entry.name.lower().endswith(supported_formats):
                image_files.append(Path(entry.path))
        
        logger.info(f"Found {len(image_files)} image files to process")
        
        # Process files with thread pool
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.process_photo, img): img for img in image_files}
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in thread: {e}")
        
        logger.info("Processing complete!")

if __name__ == '__main__':
    classifier = PhotoClassifier()
    classifier.run()
