import os
import sqlite3
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
import sys

try:
    from PIL import Image
    from PIL.ExifTags import TAGS, GPSTAGS
except ImportError:
    print("Error: Pillow not installed. Install with: pip install Pillow")
    sys.exit(1)

# Register HEIC support BEFORE any Image operations
try:
    import pillow_heif
    pillow_heif.register_heif_opener()
    logger_init = logging.getLogger(__name__)
    logger_init.info("pillow-heif registered successfully")
except ImportError:
    print("Warning: pillow-heif not installed. HEIC files will be skipped.")
    print("Install with: pip install pillow-heif")
except Exception as e:
    print(f"Warning: Failed to register pillow-heif: {e}")

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
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
    
    def is_valid_heic(self, file_path):
        """Check if file has HEIC magic bytes"""
        try:
            with open(file_path, 'rb') as f:
                header = f.read(12)
                # HEIC files should have 'ftyp' at offset 4
                if len(header) >= 8:
                    if header[4:8] == b'ftyp':
                        logger.debug(f"Valid HEIC header found in {file_path}")
                        return True
                logger.warning(f"Invalid HEIC header in {file_path}")
                return False
        except Exception as e:
            logger.warning(f"Cannot check HEIC validity for {file_path}: {e}")
            return False
    
    def extract_exif(self, image_path):
        """Extract EXIF data from image, focusing on date and GPS"""
        exif_data = {
            'date': None,
            'latitude': None,
            'longitude': None,
            'camera_model': None
        }
        
        try:
            # For HEIC files, validate format first
            if image_path.suffix.lower() == '.heic':
                if not self.is_valid_heic(image_path):
                    logger.warning(f"Invalid HEIC file: {image_path.name}")
                    return exif_data
            
            logger.debug(f"Opening image: {image_path.name}")
            image = Image.open(image_path)
            logger.debug(f"Image format: {image.format}")
            
            # Try different methods to get EXIF
            exif = None
            try:
                exif = image.getexif()
                if exif:
                    logger.debug(f"EXIF obtained via getexif()")
            except Exception as e:
                logger.debug(f"getexif() failed: {e}")
                try:
                    exif = image._getexif()
                    if exif:
                        logger.debug(f"EXIF obtained via _getexif()")
                except Exception as e2:
                    logger.debug(f"_getexif() also failed: {e2}")
            
            if not exif:
                logger.debug(f"No EXIF data found in {image_path.name}")
                return exif_data
            
            logger.debug(f"EXIF keys for {image_path.name}: {list(exif.keys())}")
            
            for tag_id, value in exif.items():
                tag_name = TAGS.get(tag_id, tag_id)
                
                # Extract datetime
                if tag_name == 'DateTime':
                    exif_data['date'] = value
                    logger.debug(f"Found DateTime: {value}")
                
                # Extract GPS info
                elif tag_name == 'GPSInfo':
                    gps_data = self.parse_gps_ifd(value)
                    if gps_data:
                        exif_data['latitude'] = gps_data[0]
                        exif_data['longitude'] = gps_data[1]
                        logger.debug(f"Found GPS: {gps_data}")
                
                # Extract camera model
                elif tag_name == 'Model':
                    exif_data['camera_model'] = value
                    logger.debug(f"Found Model: {value}")
            
        except Exception as e:
            logger.error(f"Error reading EXIF from {image_path}: {e}", exc_info=True)
        
        return exif_data
    
    def parse_gps_ifd(self, gps_ifd):
        """Parse GPS IFD to get latitude and longitude"""
        try:
            gps_data = {}
            
            for tag_id, value in gps_ifd.items():
                tag_name = GPSTAGS.get(tag_id, tag_id)
                gps_data[tag_name] = value
            
            logger.debug(f"GPS data: {gps_data}")
            
            if 'GPSLatitude' not in gps_data or 'GPSLongitude' not in gps_data:
                return None
            
            lat = self.convert_to_degrees(gps_data['GPSLatitude'])
            lon = self.convert_to_degrees(gps_data['GPSLongitude'])
            
            # Apply direction
            if gps_data.get('GPSLatitudeRef') == 'S':
                lat = -lat
            if gps_data.get('GPSLongitudeRef') == 'W':
                lon = -lon
            
            return (lat, lon)
        except Exception as e:
            logger.debug(f"Error parsing GPS IFD: {e}")
            return None
    
    @staticmethod
    def convert_to_degrees(value):
        """Convert GPS coordinates to degrees"""
        try:
            d = value[0]
            m = value[1]
            s = value[2]
            
            # Handle both float and Fraction types
            if hasattr(d, 'numerator'):
                d = d.numerator / d.denominator
            if hasattr(m, 'numerator'):
                m = m.numerator / m.denominator
            if hasattr(s, 'numerator'):
                s = s.numerator / s.denominator
            
            return float(d) + (float(m) / 60.0) + (float(s) / 3600.0)
        except Exception as e:
            logger.debug(f"Error converting GPS coordinates: {e}")
            return None
    
    def get_date_from_filename(self, filename):
        """Try to extract date from filename as fallback"""
        try:
            # Try YYYY-MM-DD HH.MM.SS format (common for iPhone)
            if ' ' in filename:
                date_part = filename.split(' ')[0]
                return datetime.strptime(date_part, '%Y-%m-%d').strftime('%Y-%m-%d')
            
            # Try YYYYMMDD format
            parts = filename.split('_')
            for part in parts:
                if len(part) >= 8 and part[:8].isdigit():
                    return datetime.strptime(part[:8], '%Y%m%d').strftime('%Y-%m-%d')
        except Exception as e:
            logger.debug(f"Error extracting date from filename {filename}: {e}")
        
        return None
    
    def process_photo(self, image_path):
        """Process a single photo: extract EXIF, classify by date, store metadata"""
        try:
            logger.info(f"Processing: {image_path.name}")
            exif_data = self.extract_exif(image_path)
            taken_date = exif_data['date']
            
            # Fallback to filename if no EXIF date found
            if not taken_date:
                taken_date = self.get_date_from_filename(image_path.name)
                if taken_date:
                    logger.info(f"Using date from filename for {image_path.name}: {taken_date}")
            
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
            
            logger.info(f"Successfully processed: {image_path.name} -> {new_path}")
            
        except Exception as e:
            logger.error(f"Error processing {image_path.name}: {e}", exc_info=True)
    
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
                    logger.error(f"Error in thread: {e}", exc_info=True)
        
        logger.info("Processing complete!")

if __name__ == '__main__':
    classifier = PhotoClassifier()
    classifier.run()
