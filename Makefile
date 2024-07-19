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
	cd lib/nodejs && npm ci && npm run release -ws

build-python-rust:
	cd lib/rust/detection_lib && $(MAKE) release

build-python: build-python-rust
	rm -rf local-assets/MatanoDetectionsCommonLayer && cd lib/python/matano_detection && $(MAKE) release

build-rust-linux-x64:
	cd lib/rust && PYO3_CROSS_PYTHON_VERSION=3.9 cargo lambda build --target x86_64-unknown-linux-gnu.2.26 --release --workspace && \
	mkdir -p ${CURDIR}/local-assets && \
	cp -a target/lambda/* ${CURDIR}/local-assets

build-rust-linux-arm:
	cd lib/rust && PYO3_CROSS_PYTHON_VERSION=3.9 cargo lambda build --target aarch64-unknown-linux-gnu.2.26 --release --workspace && \
	mkdir -p ${CURDIR}/local-assets && \
	cp -a target/lambda/* ${CURDIR}/local-assets

build-rust-macos-x64:
	cd lib/rust && PYO3_CROSS_PYTHON_VERSION=3.9 cargo lambda build --target x86_64-apple-darwin --release --workspace && \
	mkdir -p ${CURDIR}/local-assets && \
	cp -a target/lambda/* ${CURDIR}/local-assets

build-rust-macos-arm:
	cd lib/rust && PYO3_CROSS_PYTHON_VERSION=3.9 cargo lambda build --target aarch64-apple-darwin --release --workspace && \
	mkdir -p ${CURDIR}/local-assets && \
	cp -a target/lambda/* ${CURDIR}/local-assets

build-rust-linux: build-rust-linux-x64 build-rust-linux-arm
build-rust-macos: build-rust-macos-x64 build-rust-macos-arm
build-rust: build-rust-linux build-rust-macos

build-jvm:
	cd lib/java/matano && ./gradlew release

build-assets: build-python build-rust build-jvm

build-all: build-cli build-infra build-assets

package: build-all
	cd scripts/packaging && npm install && cd ${CURDIR} && node scripts/packaging/build.js

local-install: build-cli
	cd cli && npm run full-install
