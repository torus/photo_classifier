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

# Copies the newest SAMPLE_COUNT photos from SAMPLE_SRC (default: PHOTOS_DIR
# in .env) into ./samplephotos. Files are copied; the source is not changed.
# Override with e.g. `make run SAMPLE_COUNT=10 SAMPLE_SRC=/some/folder`.
SAMPLE_SRC ?= $(shell sed -n 's/^PHOTOS_DIR=//p' .env 2>/dev/null)
SAMPLE_COUNT ?= 40

setup-samples:
	@test -d "$(SAMPLE_SRC)" || { echo "Sample source not found: '$(SAMPLE_SRC)'. Set PHOTOS_DIR in .env or pass SAMPLE_SRC=..."; exit 1; }
	rm -rf ./samplephotos ./classified ./db
	mkdir -p ./samplephotos ./classified ./db
	ls "$(SAMPLE_SRC)" | grep -iE '\.(heic|jpe?g|png)$$' | sort | tail -n $(SAMPLE_COUNT) | \
	  while IFS= read -r f; do cp -p "$(SAMPLE_SRC)/$$f" ./samplephotos/; done
	@echo "Copied $$(ls ./samplephotos | wc -l | tr -d ' ') sample photos from $(SAMPLE_SRC)"
