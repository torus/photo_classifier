# CLAUDE.md

Photo classifier: moves photos from one flat camera-upload folder into `YYYY/MM/DD`
folders and records them in SQLite; a query script finds photos near a city. See
README.md for usage.

## Layout

- `photo_classifier.py`: the classifier. `PhotoClassifier.run()` scans `/photos`
  (not recursive) and processes files in a `ThreadPoolExecutor` (`MAX_WORKERS = 4`).
- `query_photos_by_city.py`: geocodes a city with OpenStreetMap Nominatim (stdlib
  `urllib`, generic User-Agent `photo_classifier/1.0`), or takes `--lat/--lon`.
- `docker-compose.yml`: mounts host folders from `PHOTOS_DIR`, `CLASSIFIED_DIR`, `DB_DIR`
  (from `.env`, defaults `./samplephotos`, `./classified`, `./db`) as `/photos`,
  `/classified`, `/db`, and sets `DB_PATH=/db/photos.db`. The repo is mounted at `/app`;
  the image doesn't copy the code.
- `Makefile`: `run` (samples only), `dry-run`, `query CITY=... [RADIUS=...]`,
  `setup-samples`. `NO_ENV=1` adds `--env-file /dev/null`.

## Design rules

These came from real incidents or explicit decisions; keep them when changing code.

- **Never overwrite a photo.** If the target exists, log a `Duplicate` warning and skip.
- **DB row and move are one transaction.** Insert inside `with conn:`, then
  `shutil.move` inside the same block, so a failed move rolls back the row. Don't move
  first and insert after.
- **`--dry-run` touches nothing**: no `mkdir`, no DB creation, no moves. Dry-run prints go
  through `_print_lock`, because `print()` from worker threads can merge lines.
- **Taken date order:** EXIF `DateTimeOriginal` (Exif IFD `0x8769`, tag `0x9003`), then
  IFD0 `DateTime` (that's the modified time), then the date in the filename. Dates are
  local time where the photo was taken, so they can differ from Dropbox filenames,
  which use the uploader's time zone.
- **`make run` must never use the real folders.** It always passes
  `--env-file /dev/null`. `setup-samples` reads only `PHOTOS_DIR` from `.env` with
  `sed`, on purpose: `include .env` would turn every `.env` value into a make variable
  and could leak real paths into Compose.
- **Read-only access when only reading.** The query script opens the DB with
  `file:...?mode=ro`. The real DB may live in a synced folder (Dropbox).
- `DB_PATH` is read from the environment in both scripts, defaulting to
  `/classified/photos.db`, so DBs from older versions still work.

## Testing

- No test suite. Pillow isn't installed on the host; run things in Docker.
- Unit-level checks: create a venv in a scratch directory, `pip install Pillow`, import
  `photo_classifier`, and point `PHOTOS_DIR`, `CLASSIFIED_DIR`, `DB_PATH` at temp folders.
  Generate JPEGs with EXIF via `Image.getexif()` / `get_ifd(0x8769)`. Simulate a failed
  move by patching `photo_classifier.shutil.move`.
- End to end: `make run SAMPLE_COUNT=...`, then inspect `./classified` and `./db/photos.db`.
- Before a real run: `make dry-run`, back up the real `photos.db`, and confirm the
  user wants the real run. It moves files out of their camera-upload folder.

## Local configuration

`.env` holds the user's real paths and is gitignored. Keep personal paths out of
committed files; document new settings in `.env.example`.

## Known gaps

- Images are not closed after reading EXIF (`Image.open` without `with`).
- Bare `except:` around `getexif()`.
- The per-thread HEIF registration (`ensure_heif_support`) and reading HEIC into
  `BytesIO` aren't needed; `register_heif_opener()` is process-wide.
- No summary output: logging is at WARNING, so a clean run prints nothing.
- Videos (`.mov`, `.mp4`) and `.webp` are not handled.
