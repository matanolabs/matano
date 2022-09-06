
clean: clean-infra clean-cli
	rm -rf scripts/packaging/node_modules

clean-infra:
	cd infra && npm run clean && rm -rf cdk.out

clean-cli:
	cd cli && npm run clean

build-infra:
	cd infra && npm ci && npm run build

build-cli:
	cd cli && npm ci && npm run build

build-nodejs:
	cd lib/nodejs && npm run release -ws

build-python:
	cd lib/python/matano_detection && $(MAKE) release

build-rust:
	cd lib/rust && cargo lambda build --release --workspace && \
	cp -a target/lambda/* ${CURDIR}/local-assets

build-jvm:
	cd lib/java/matano && ./gradlew release

build-assets: build-nodejs build-python build-rust build-jvm

package: build-cli build-infra build-assets
	cd scripts/packaging && npm install pkg@5.8.0 && cd ${CURDIR} && node scripts/packaging/build.js

local-install: build-cli
	cd cli && npm run full-install
