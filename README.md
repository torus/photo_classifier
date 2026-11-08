# photo_classifier

Sorts photos from a single camera-upload folder (such as Dropbox Camera Uploads) into
`YYYY/MM/DD` folders by the date they were taken, and records each photo in an SQLite
database. A second script finds photos taken in or near a city by their GPS location.

Everything runs in Docker, so the only requirement on the host is Docker with Compose.

## Setup

Copy the example settings and set your own folders:

```sh
cp .env.example .env
```

| Variable | Mounted as | Contents |
|---|---|---|
| `PHOTOS_DIR` | `/photos` | Photos to classify. They are **moved** out of this folder. |
| `CLASSIFIED_DIR` | `/classified` | Where the `YYYY/MM/DD` folders are created. |
| `DB_DIR` | `/db` | Folder for `photos.db`. |

`.env` is ignored by git. If a variable isn't set, the sample folders in this
repository (`./samplephotos`, `./classified`, `./db`) are used.

## Classifying photos

Always preview first:

```sh
make dry-run
```

This prints where each photo would go, and doesn't move files or write to the database.
When it looks right, run it for real:

```sh
docker compose run --rm main python photo_classifier.py
```

For each photo, the date comes from EXIF `DateTimeOriginal`, then EXIF `DateTime`, then
the date in the filename (for example `2026-11-01 10.26.32.heic`). GPS coordinates and
the camera model are stored in the database when present.

- Supported formats: `.jpg`, `.jpeg`, `.heic`, `.png`, `.gif`, `.bmp`. Videos and other
  files are left where they are.
- If a file with the same name already exists in the target folder, the photo is skipped
  with a `Duplicate` warning. Nothing is overwritten.
- The database row and the move happen together: if the move fails, no row is recorded.
- Dates are the local time where the photo was taken, so a photo taken abroad near
  midnight can land on a different day than its filename suggests.

## Finding photos by city

```sh
make query CITY=Valencia
make query CITY="Valencia, Spain" RADIUS=30
```

The city's coordinates are looked up with
[OpenStreetMap Nominatim](https://nominatim.openstreetmap.org/), which needs a network
connection. Check the place name printed at the top, and add the country if it picked
the wrong place. To skip the lookup, give coordinates directly:

```sh
docker compose run --rm main python query_photos_by_city.py Guam --lat 13.51 --lon 144.81 --radius 10
```

The output lists photo counts per day, then one line per photo, oldest first. Paths are
shown as stored in the database (`/classified/...`), relative to your `CLASSIFIED_DIR`.

## Testing with sample photos

```sh
make run                    # copy the newest 40 photos and classify them
make run SAMPLE_COUNT=10    # use fewer photos
make dry-run NO_ENV=1       # preview on the sample folders
make query CITY=Tokyo NO_ENV=1
```

`make run` copies photos from `PHOTOS_DIR` (or `SAMPLE_SRC=...`) into `./samplephotos`;
the originals are not changed. It clears `./samplephotos`, `./classified` and `./db`
first, and never uses the folders in `.env`. Add `NO_ENV=1` to any other target to
ignore `.env` the same way.

## Database

`photos.db` has one table:

| Column | Description |
|---|---|
| `filename` | Original file name |
| `original_path`, `new_path` | Paths inside the container (`/photos/...`, `/classified/...`) |
| `taken_date` | ISO date and time, local time where the photo was taken |
| `latitude`, `longitude` | Decimal degrees, or NULL without GPS |
| `camera_model` | EXIF model, for example `iPhone 16` |
| `processed_date` | When the photo was classified (UTC) |

Back up the database before large runs. If it lives in a synced folder such as Dropbox,
consider pausing sync while the classifier is writing to it.
