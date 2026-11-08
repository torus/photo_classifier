# Set NO_ENV=1 to ignore .env and use the sample folders
# (the defaults in docker-compose.yml), e.g. `make dry-run NO_ENV=1`.
COMPOSE = docker compose $(if $(NO_ENV),--env-file /dev/null)

.PHONY: run dry-run query setup-samples

# Always runs on the sample folders, never on the paths in .env.
run: setup-samples
	docker compose --env-file /dev/null up

dry-run:
	$(COMPOSE) run --rm main python photo_classifier.py --dry-run

query:
	$(COMPOSE) run --rm main python query_photos_by_city.py

setup-samples:
	rm -rf ./samplephotos/*
	mkdir -p ./samplephotos
	rm -rf ./classified/*
	mkdir -p ./classified
	rm -rf ./db/*
	mkdir -p ./db
	cp /Volumes/2TB/Dropbox/カメラアップロード/2026-02-*.heic ./samplephotos
