

build:
	cd infra && npm run clean && npm ci && npm run build

install: build
	cd cli && npm run clean && npm run full-install
