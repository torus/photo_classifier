SAMPLE_ENV = PHOTOS_DIR=./samplephotos CLASSIFIED_DIR=./classified DB_DIR=./db

run: setup-samples
	$(SAMPLE_ENV) docker compose up

setup-samples:
	rm -rf ./samplephotos/*
	mkdir -p ./samplephotos
	rm -rf ./classified/*
	mkdir -p ./classified
	rm -rf ./db/*
	mkdir -p ./db
	cp /Volumes/2TB/Dropbox/カメラアップロード/2026-02-*.heic ./samplephotos
