import argparse
import os
import sqlite3
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
import sys
import io
from threading import local, Lock

try:
    from PIL import Image
    from PIL.ExifTags import TAGS, GPSTAGS
except ImportError:
    print("Error: Pillow not installed. Install with: pip install Pillow")
    sys.exit(1)

# Register HEIC support at module level
heif_available = False
try:
    import pillow_heif
    heif_available = True
except ImportError:
    print("Warning: pillow-heif not installed. HEIC files will be skipped.")
    print("Install with: pip install pillow-heif")

# Setup logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PHOTOS_DIR = Path('/photos')
CLASSIFIED_DIR = Path('/classified')
DB_PATH = Path(os.environ.get('DB_PATH', '/classified/photos.db'))
MAX_WORKERS = 4

# Thread-local storage for HEIF opener registration
_thread_local = local()

# print() writes the text and the newline separately, so output from
# worker threads can end up on one line without a lock.
_print_lock = Lock()

def ensure_heif_support():
    """Ensure HEIF support is registered in current thread"""
    if not heif_available:
        return
    
    if not getattr(_thread_local, 'heif_registered', False):
        try:
            import pillow_heif
            pillow_heif.register_heif_opener()
            _thread_local.heif_registered = True
        except Exception as e:
            logger.warning(f"Failed to register HEIF support: {e}")

class PhotoClassifier:
    def __init__(self, dry_run=False):
        self.db_path = DB_PATH
        self.dry_run = dry_run
        if not dry_run:
            CLASSIFIED_DIR.mkdir(parents=True, exist_ok=True)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
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
                if len(header) >= 8 and header[4:8] == b'ftyp':
                    return True
                logger.warning(f"Invalid HEIC header: {file_path.name}")
                return False
        except Exception as e:
            logger.warning(f"Cannot check HEIC: {e}")
            return False
    
    def extract_exif(self, image_path):
        """Extract EXIF data from image"""
        exif_data = {
            'date': None,
            'latitude': None,
            'longitude': None,
            'camera_model': None
        }
        
        try:
            # Ensure HEIF support for this thread
            ensure_heif_support()
            
            # For HEIC, read file into memory first
            if image_path.suffix.lower() == '.heic':
                if not self.is_valid_heic(image_path):
                    return exif_data
                
                try:
                    with open(image_path, 'rb') as f:
                        image_bytes = io.BytesIO(f.read())
                    image = Image.open(image_bytes)
                except Exception as e:
                    logger.warning(f"Failed to open HEIC {image_path.name}: {e}")
                    return exif_data
            else:
                image = Image.open(image_path)
            
            # Extract EXIF - use getexif() with IFD access for HEIC
            try:
                exif = image.getexif()
            except:
                try:
                    exif = image._getexif()
                except:
                    exif = None
            
            if not exif:
                return exif_data
            
            date_time = None
            for tag_id, value in exif.items():
                tag_name = TAGS.get(tag_id, tag_id)

                if tag_name == 'DateTime':
                    date_time = value
                elif tag_name == 'Model':
                    exif_data['camera_model'] = value

            # Prefer DateTimeOriginal (capture time, in the Exif sub-IFD 0x8769)
            # over IFD0 DateTime, which is the last-modified time.
            try:
                date_original = exif.get_ifd(0x8769).get(0x9003)
            except Exception:
                date_original = None
            exif_data['date'] = date_original or date_time
            
            # Try to get GPS from IFD (works better with HEIC)
            try:
                ifd = exif.get_ifd(0x8825)  # 0x8825 is GPSInfo tag
                if ifd:
                    gps_data = self.parse_gps_ifd(ifd)
                    if gps_data:
                        exif_data['latitude'] = gps_data[0]
                        exif_data['longitude'] = gps_data[1]
            except Exception as e:
                # Fallback: try to parse GPSInfo if it exists as a dict
                if 34853 in exif:
                    gps_value = exif[34853]
                    if isinstance(gps_value, dict):
                        gps_data = self.parse_gps_ifd(gps_value)
                        if gps_data:
                            exif_data['latitude'] = gps_data[0]
                            exif_data['longitude'] = gps_data[1]
            
        except Exception as e:
            logger.error(f"Error reading EXIF from {image_path}: {e}")
        
        return exif_data
    
    def parse_gps_ifd(self, gps_ifd):
        """Parse GPS IFD"""
        try:
            if not isinstance(gps_ifd, dict):
                return None
                
            gps_data = {}
            for tag_id, value in gps_ifd.items():
                tag_name = GPSTAGS.get(tag_id, tag_id)
                gps_data[tag_name] = value
            
            if 'GPSLatitude' not in gps_data or 'GPSLongitude' not in gps_data:
                return None
            
            lat = self.convert_to_degrees(gps_data['GPSLatitude'])
            lon = self.convert_to_degrees(gps_data['GPSLongitude'])
            
            if lat is None or lon is None:
                return None
            
            if gps_data.get('GPSLatitudeRef') == 'S':
                lat = -lat
            if gps_data.get('GPSLongitudeRef') == 'W':
                lon = -lon
            
            return (lat, lon)
        except Exception as e:
            logger.error(f"Error parsing GPS: {e}")
            return None
    
    @staticmethod
    def convert_to_degrees(value):
        """Convert GPS coordinates to degrees"""
        try:
            d, m, s = value[0], value[1], value[2]
            
            if hasattr(d, 'numerator'):
                d = d.numerator / d.denominator
            if hasattr(m, 'numerator'):
                m = m.numerator / m.denominator
            if hasattr(s, 'numerator'):
                s = s.numerator / s.denominator
            
            result = float(d) + (float(m) / 60.0) + (float(s) / 3600.0)
            return result
        except Exception as e:
            logger.error(f"Error converting degrees: {e}")
            return None
    
    def get_date_from_filename(self, filename):
        """Extract date from filename"""
        try:
            if ' ' in filename:
                date_part = filename.split(' ')[0]
                return datetime.strptime(date_part, '%Y-%m-%d').strftime('%Y-%m-%d')
            
            parts = filename.split('_')
            for part in parts:
                if len(part) >= 8 and part[:8].isdigit():
                    return datetime.strptime(part[:8], '%Y%m%d').strftime('%Y-%m-%d')
        except Exception as e:
            pass
        
        return None
    
    def process_photo(self, image_path):
        """Process a single photo"""
        try:
            exif_data = self.extract_exif(image_path)
            taken_date = exif_data['date']
            
            if not taken_date:
                taken_date = self.get_date_from_filename(image_path.name)
            
            if not taken_date:
                logger.warning(f"No date for {image_path.name}, skipping")
                return
            
            if isinstance(taken_date, str) and ':' in taken_date:
                date_obj = datetime.strptime(taken_date, '%Y:%m:%d %H:%M:%S')
            else:
                date_obj = datetime.strptime(taken_date, '%Y-%m-%d')
            
            year = date_obj.strftime('%Y')
            month = date_obj.strftime('%m')
            day = date_obj.strftime('%d')
            
            target_dir = CLASSIFIED_DIR / year / month / day
            new_path = target_dir / image_path.name
            if new_path.exists():
                logger.warning(
                    f"Duplicate: {new_path} already exists, skipping {image_path}"
                )
                return

            if self.dry_run:
                with _print_lock:
                    print(f"[dry-run] {image_path} -> {new_path}")
                return

            target_dir.mkdir(parents=True, exist_ok=True)

            # Insert into the DB first, then move. The transaction is committed
            # only if the move succeeds; otherwise it is rolled back, so the DB
            # never records a photo that wasn't moved.
            conn = sqlite3.connect(self.db_path, timeout=30)
            try:
                with conn:  # commits on success, rolls back on exception
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
                    shutil.move(str(image_path), str(new_path))
            finally:
                conn.close()
            
        except Exception as e:
            logger.error(f"Error processing {image_path.name}: {e}")
    
    def run(self):
        """Main execution"""
        if not PHOTOS_DIR.exists():
            logger.error(f"Photos directory not found: {PHOTOS_DIR}")
            return

        image_files = []
        supported_formats = ('.jpg', '.jpeg', '.heic', '.png', '.gif', '.bmp')
        
        for entry in os.scandir(PHOTOS_DIR):
            if entry.is_file() and entry.name.lower().endswith(supported_formats):
                image_files.append(Path(entry.path))
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.process_photo, img): img for img in image_files}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Thread error: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Classify photos into YYYY/MM/DD folders')
    parser.add_argument('--dry-run', action='store_true',
                        help='show what would be moved without moving files or writing the DB')
    args = parser.parse_args()

    classifier = PhotoClassifier(dry_run=args.dry_run)
    classifier.run()
