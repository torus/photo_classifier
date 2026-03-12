run: setup-samples
	docker compose up

setup-samples:
	rm -rf ./samplephotos/*
	mkdir -p ./samplephotos
	rm -rf ./classified/*
	mkdir -p ./classified
	cp /Volumes/2TB/Dropbox/カメラアップロード/2026-02-*.heic ./samplephotos
